# Getting Started with Create React App and Fluent UI

This is a [Create React App](https://github.com/facebook/create-react-app) based repo that comes with Fluent UI pre-installed!

## Available Scripts

In the project directory, you can run:

### `npm start`

Runs the app in the development mode.<br>
Open [http://localhost:3000](http://localhost:3000) to view it in the browser.

The page will reload if you make edits.<br>
You will also see any lint errors in the console.

### `npm test`

Launches the test runner in the interactive watch mode.<br>
See the section about [running tests](https://facebook.github.io/create-react-app/docs/running-tests) for more information.

### `npm run build`

Builds the app for production to the `build` folder.<br>
It correctly bundles React in production mode and optimizes the build for the best performance.

The build is minified and the filenames include the hashes.<br>
Your app is ready to be deployed!

See the section about [deployment](https://facebook.github.io/create-react-app/docs/deployment) for more information.

### `npm run eject`

**Note: this is a one-way operation. Once you `eject`, you can’t go back!**

If you aren’t satisfied with the build tool and configuration choices, you can `eject` at any time. This command will remove the single build dependency from your project.

Instead, it will copy all the configuration files and the transitive dependencies (Webpack, Babel, ESLint, etc) right into your project so you have full control over them. All of the commands except `eject` will still work, but they will point to the copied scripts so you can tweak them. At this point you’re on your own.

You don’t have to ever use `eject`. The curated feature set is suitable for small and middle deployments, and you shouldn’t feel obligated to use this feature. However we understand that this tool wouldn’t be useful if you couldn’t customize it when you are ready for it.

## Learn More

You can learn more in the [Create React App documentation](https://facebook.github.io/create-react-app/docs/getting-started).

To learn React, check out the [React documentation](https://reactjs.org/).

# Contributing

This project welcomes contributions and suggestions. Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## API 客户端请求平滑（Qwen3-Coder）

`src/api/rateLimiter.ts` 提供 `RampRateLimiter`：将瞬时涌入的请求整形为随时间
平稳扩展的发送流，避免请求率爬升过快触发服务端限流（429）。

- **冷启动斜坡**：发射速率从稳态的 25%（`rampStartFraction`）在 `rampDurationMs`
  内线性爬到 100%，并发额度同步从 `minConcurrent` 爬坡到 `maxConcurrent`；
- **令牌桶 + 滑动窗口闸门**：双保险限制任意 1s 窗口内的真实发射数；
- **AIMD 反馈**：收到 429/5xx 时乘性收缩并尊重 `Retry-After`，成功后加性恢复，
  且恢复重新经历线性爬坡；重试带指数退避与抖动，绝不立即重发。

用法：

```ts
import { qwenCoderFetch } from './api/rateLimiter';

const res = await qwenCoderFetch('/v1/chat/completions', { method: 'POST', body });
```

实测：100 个突发请求经默认参数整形后，各 500ms 窗口的发射数为
4 → 6 → 6 → 9 → 10 → 12 → 12 → 12 → 13（平滑线性爬升，无阶跃尖峰）。
