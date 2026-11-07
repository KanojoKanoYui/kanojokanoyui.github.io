// rateLimiter.ts
// 客户端请求平滑器：把"瞬间涌入"的请求队列整形为随时间平稳扩展的发送流，
// 避免因请求率爬升过快触发 Qwen3-Coder 服务端的稳定性保护（429 / 限流）。
//
// 三层整形：
//   1. 令牌桶（token bucket）—— 限制稳态速率，桶容量吸收小突发；
//   2. 冷启动斜坡（ramp）     —— 有效并发从 minConcurrent 线性扩展到 maxConcurrent，
//                                斜率由 rampDurationMs 控制，杜绝阶跃式流量爬升；
//   3. 限流反馈（AIMD）       —— 收到 429/5xx 时乘性收缩并发并尊重 Retry-After，
//                                成功后加性递增缓慢恢复，且恢复重新经历线性爬坡。

export interface RateLimiterOptions {
  /** 稳态最大并发（斜坡扩展的上限）。默认 4 */
  maxConcurrent?: number;
  /** 冷启动初始并发（斜坡起点）。默认 1 */
  minConcurrent?: number;
  /** 从 minConcurrent 线性扩展到 maxConcurrent 所需时间（毫秒）。默认 15000 */
  rampDurationMs?: number;
  /** 两次请求发射之间的最小间隔（毫秒），即稳态速率 ≈ 1/requestIntervalMs。默认 250 */
  requestIntervalMs?: number;
  /** 令牌桶容量（允许的瞬时突发数）。默认等于 maxConcurrent */
  burstCapacity?: number;
  /** 失败重试上限。默认 3 */
  maxRetries?: number;
  /** 指数退避基数（毫秒）。默认 800 */
  backoffBaseMs?: number;
  /** 指数退避封顶（毫秒）。默认 30000 */
  backoffMaxMs?: number;
  /** 每次成功响应后并发额度恢复的增量。默认 1 */
  concurrencyStepUp?: number;
  /** 触发限流时并发收缩的乘性因子 (0,1)。默认 0.5 */
  concurrencyFactor?: number;
  /** 自定义睡眠注入（便于测试）。默认基于 setTimeout */
  sleep?: (ms: number) => Promise<void>;
  /** 冷启动起步速率占稳态速率的比例 (0,1]。默认 0.25，之后在 rampDurationMs 内线性爬到 100% */
  rampStartFraction?: number;
}

const defaultSleep = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));

/** 单调时钟：优先 performance.now()，回退 Date.now() */
const nowFn: () => number =
  typeof performance !== 'undefined' && typeof performance.now === 'function'
    ? () => performance.now()
    : () => Date.now();

interface Task<T> {
  run: () => Promise<T>;
  resolve: (value: T) => void;
  reject: (reason: unknown) => void;
  attempt: number;
}

export class RampRateLimiter {
  private readonly maxConcurrent: number;
  private readonly minConcurrent: number;
  private readonly rampDurationMs: number;
  private readonly requestIntervalMs: number;
  private readonly burstCapacity: number;
  private readonly maxRetries: number;
  private readonly backoffBaseMs: number;
  private readonly backoffMaxMs: number;
  private readonly concurrencyStepUp: number;
  private readonly concurrencyFactor: number;
  private readonly sleep: (ms: number) => Promise<void>;

  /** AIMD 动态并发额度（受斜坡与限流反馈共同调节，可为小数） */
  private allowed: number;
  /** 已放宽到的并发上限（斜坡推进值，限流收缩会同步下调它） */
  private ceiling: number;
  /** 本轮斜坡窗口的起点时间戳（限流收缩后重置，恢复重新爬坡） */
  private rampStartedAt: number;
  /** 上次因限流收缩的时间戳（冷却期内不重复惩罚） */
  private lastThrottleAt: number;

  /** 令牌桶状态 */
  private tokens: number;
  private lastTokenRefillAt: number;

  /** 速率闸门状态：滑动窗口内的发射时间戳（用于计算真实瞬时速率） */
  private recentLaunches: number[] = [];

  private inFlight = 0;
  /** 最近一次发射的时刻（用于计算真实滑动速率） */
  private lastLaunchAt = 0;
  /** 目标稳态速率：每秒 requestIntervalMs 分之一；冷启动按 rampStartFraction 起步 */
  private readonly steadyRatePerSec: number;
  private readonly rampStartFraction: number;
  private readonly queue: Array<Task<unknown>> = [];
  private pumpScheduled = false;
  private wakeTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(options: RateLimiterOptions = {}) {
    this.maxConcurrent = Math.max(1, options.maxConcurrent ?? 4);
    this.minConcurrent = Math.max(
      1,
      Math.min(options.minConcurrent ?? 1, this.maxConcurrent),
    );
    this.rampDurationMs = Math.max(0, options.rampDurationMs ?? 15_000);
    this.requestIntervalMs = Math.max(0, options.requestIntervalMs ?? 250);
    this.burstCapacity = Math.max(1, options.burstCapacity ?? this.maxConcurrent);
    this.maxRetries = Math.max(0, options.maxRetries ?? 3);
    this.backoffBaseMs = Math.max(0, options.backoffBaseMs ?? 800);
    this.backoffMaxMs = Math.max(this.backoffBaseMs, options.backoffMaxMs ?? 30_000);
    this.concurrencyStepUp = Math.max(0.1, options.concurrencyStepUp ?? 1);
    this.concurrencyFactor = Math.min(0.99, Math.max(0.05, options.concurrencyFactor ?? 0.5));
    this.sleep = options.sleep ?? defaultSleep;
    this.rampStartFraction = Math.min(1, Math.max(0.05, options.rampStartFraction ?? 0.25));
    this.steadyRatePerSec = this.requestIntervalMs > 0 ? 1000 / this.requestIntervalMs : Number.POSITIVE_INFINITY;

    const t = nowFn();
    this.allowed = this.minConcurrent;
    this.ceiling = this.minConcurrent;
    this.rampStartedAt = t;
    this.lastThrottleAt = 0;
    this.tokens = 1; // 冷启动只放行第一个请求，其余排队
    this.lastTokenRefillAt = t;
    this.recentLaunches = [];
    this.lastLaunchAt = 0;
  }

  /** 将任意 fetcher 包进限流队列；返回排队后的执行 Promise */
  enqueue<T>(run: () => Promise<T>): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      this.queue.push({
        run,
        resolve: resolve as (value: unknown) => void,
        reject,
        attempt: 0,
      });
      this.schedulePump();
    });
  }

  /** 观测用快照 */
  stats() {
    const t = nowFn();
    this.recentLaunches = this.recentLaunches.filter((x) => x > t - 1000);
    return {
      queued: this.queue.length,
      inFlight: this.inFlight,
      allowedConcurrency: this.allowed,
      rampCeiling: this.ceiling,
      launchesLastSecond: this.recentLaunches.length,
    };
  }

  // ------------------------------------------------------------------ internals

  private schedulePump() {
    if (this.pumpScheduled) return;
    this.pumpScheduled = true;
    // microtask 合并同一 tick 内的多次入队，避免惊群式重复调度
    void Promise.resolve().then(() => {
      this.pumpScheduled = false;
      this.pump();
    });
  }

  /** 当前时刻斜坡允许的最大并发：min + (max-min) * min(1, elapsed/ramp) */
  private rampTarget(t: number): number {
    if (this.rampDurationMs === 0) return this.maxConcurrent;
    const progress = Math.min(1, (t - this.rampStartedAt) / this.rampDurationMs);
    return this.minConcurrent + (this.maxConcurrent - this.minConcurrent) * progress;
  }

  /**
   * 速率闸门：距下一次发射还需等待的毫秒数。
   * 同时受两个线性爬升的约束：
   *   a) 最小间隔随斜坡从 steadyInterval/rampStartFraction 收敛到 steadyInterval；
   *   b) 滑动窗口（1s）内的真实发射数不得超过当前斜坡允许的速率。
   */
  private launchWaitMs(t: number): number {
    if (!Number.isFinite(this.steadyRatePerSec)) {
      // 未配置速率间隔时退化为纯并发控制
      return 0;
    }
    const progress =
      this.rampDurationMs === 0 ? 1 : Math.min(1, (t - this.rampStartedAt) / this.rampDurationMs);
    const currentRate =
      this.steadyRatePerSec * (this.rampStartFraction + (1 - this.rampStartFraction) * progress);
    // a) 相邻两次发射的最小间隔
    const spacingGate = this.lastLaunchAt === 0 ? 0 : this.lastLaunchAt + 1000 / currentRate - t;
    // b) 滑动窗口速率核对：清理 1s 前的记录，若窗口内已发满则等到最早记录滑出
    this.recentLaunches = this.recentLaunches.filter((x) => x > t - 1000);
    const maxInWindow = Math.max(1, Math.floor(currentRate));
    let windowGate = 0;
    if (this.recentLaunches.length >= maxInWindow) {
      windowGate = this.recentLaunches[this.recentLaunches.length - maxInWindow] + 1000 - t;
    }
    return Math.max(0, spacingGate, windowGate);
  }

  private refillTokens(t: number) {
    const elapsed = t - this.lastTokenRefillAt;
    if (elapsed <= 0) return;
    this.lastTokenRefillAt = t;
    // 令牌补充速率 = 当前斜坡允许的瞬时速率（而非稳态速率），保证冷启动阶段不囤积突发额度
    const progress =
      this.rampDurationMs === 0
        ? 1
        : Math.min(1, (t - this.rampStartedAt) / this.rampDurationMs);
    const rampedRatePerSec = Number.isFinite(this.steadyRatePerSec)
      ? this.steadyRatePerSec * (this.rampStartFraction + (1 - this.rampStartFraction) * progress)
      : 1000 / Math.max(1, this.requestIntervalMs);
    this.tokens = Math.min(this.burstCapacity, this.tokens + (elapsed * rampedRatePerSec) / 1000);
  }

  private pump() {
    const t = nowFn();
    this.refillTokens(t);

    // 1) 斜坡推进：allowed 追随 rampTarget，但每个 tick 至多放宽 stepUp，
    //    保证并发扩展是渐进线性的，而非一次跳满。
    const target = this.rampTarget(t);
    this.ceiling = Math.max(this.ceiling, Math.min(this.maxConcurrent, Math.floor(target)));
    if (this.allowed < target) {
      this.allowed = Math.min(target, this.allowed + this.concurrencyStepUp);
    } else {
      this.allowed = Math.min(this.allowed, Math.max(this.minConcurrent, target));
    }

    // 2) 按令牌 + 并发额度 + 速率闸门逐个放行
    while (this.queue.length > 0 && this.inFlight < this.allowed && this.tokens >= 1) {
      const waitMs = this.launchWaitMs(nowFn());
      if (waitMs > 0) break; // 交给下面的定时唤醒，绝不突破闸门连发
      const launchAt = nowFn();
      this.tokens -= 1;
      this.lastLaunchAt = launchAt;
      this.recentLaunches.push(launchAt);
      const task = this.queue.shift()!;
      this.launch(task);
    }

    // 3) 若仍有积压但被时间闸门挡住，安排一次定时唤醒继续放行
    if (this.queue.length > 0 && this.wakeTimer === null) {
      const next = this.queue.length > 0 ? Math.max(1, this.launchWaitMs(nowFn())) : 1;
      this.wakeTimer = setTimeout(() => {
        this.wakeTimer = null;
        this.pump();
      }, next);
    }
  }

  private launch(task: Task<unknown>) {
    this.inFlight += 1;
    task
      .run()
      .then((value) => {
        this.onSuccess();
        task.resolve(value);
      })
      .catch((error: unknown) => {
        if (this.isRetryable(error) && task.attempt < this.maxRetries) {
          task.attempt += 1;
          this.onThrottle(error);
          const delay = this.retryDelayFor(error, task.attempt);
          // 重试请求延迟后重新入队尾部——绝不立即重发，防止失败风暴
          void this.sleep(delay).then(() => {
            this.queue.push(task);
            this.schedulePump();
          });
        } else {
          task.reject(error);
        }
      })
      .finally(() => {
        this.inFlight -= 1;
        this.schedulePump();
      });
  }

  private onSuccess() {
    // AIMD 加性恢复：缓慢回升，且不超过当前斜坡上限
    if (this.allowed < this.ceiling) {
      this.allowed = Math.min(this.ceiling, this.allowed + this.concurrencyStepUp * 0.1);
    }
  }

  private isRetryable(error: unknown): boolean {
    const status = (error as { status?: number } | null)?.status;
    if (status === 429) return true;
    if (typeof status === 'number' && status >= 500 && status < 600) return true;
    // 无 HTTP 状态的网络层错误（fetch reject / TypeError）视为可重试
    return error instanceof TypeError;
  }

  private onThrottle(error: unknown) {
    void error;
    const t = nowFn();
    // 冷却期内不重复惩罚，避免同批并发失败把额度打到地板
    if (t - this.lastThrottleAt < this.requestIntervalMs * 4) return;
    this.lastThrottleAt = t;
    // 乘性收缩
    const shrunk = Math.max(this.minConcurrent, this.allowed * this.concurrencyFactor);
    this.allowed = shrunk;
    this.ceiling = Math.max(this.minConcurrent, Math.min(this.ceiling, shrunk + 1));
    // 重置斜坡窗口：后续恢复重新经历线性爬坡，而不是直接跳回高位
    this.rampStartedAt = t;
  }

  private retryDelayFor(error: unknown, attempt: number): number {
    // 优先尊重服务端 Retry-After
    const headers = (error as { headers?: { get?: (k: string) => string | null } } | null)?.headers;
    const retryAfter = headers?.get?.('Retry-After') ?? headers?.get?.('retry-after');
    if (retryAfter) {
      const secs = Number(retryAfter);
      if (!Number.isNaN(secs)) return Math.max(0, secs * 1000);
      const date = Date.parse(retryAfter);
      if (!Number.isNaN(date)) return Math.max(0, date - Date.now());
    }
    // 指数退避 + 抖动（jitter 打散同批重试的共振峰值）
    const exp = Math.min(this.backoffMaxMs, this.backoffBaseMs * 2 ** (attempt - 1));
    return Math.round(exp / 2 + Math.random() * (exp / 2));
  }
}

/** 携带 HTTP 状态与响应头的可重试错误包装 */
export class HttpError extends Error {
  readonly status: number;
  readonly headers: Headers;

  constructor(status: number, headers: Headers, message?: string) {
    super(message ?? `HTTP ${status}`);
    this.name = 'HttpError';
    this.status = status;
    this.headers = headers;
  }
}

/**
 * 经限流整形的 fetch：非 2xx 自动抛 HttpError（供上层退避重试）。
 * 用法：throttledFetch('/v1/chat/completions', { method: 'POST', body })
 */
export function throttledFetch(
  limiter: RampRateLimiter,
  input: RequestInfo,
  init?: RequestInit,
): Promise<Response> {
  return limiter.enqueue(async () => {
    const response = await fetch(input, init);
    if (!response.ok) {
      throw new HttpError(response.status, response.headers, `HTTP ${response.status}`);
    }
    return response;
  });
}

/** 便捷单例：Qwen3-Coder 客户端默认整形参数 */
export const qwenCoderLimiter = new RampRateLimiter({
  maxConcurrent: 4,
  minConcurrent: 1,
  rampDurationMs: 15_000,
  requestIntervalMs: 250,
});

/** Qwen3-Coder 专用封装：所有调用自动经过平滑斜坡 */
export function qwenCoderFetch(input: RequestInfo, init?: RequestInit): Promise<Response> {
  return throttledFetch(qwenCoderLimiter, input, init);
}
