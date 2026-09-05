package biz

import (
	"net/http"
	"os"
)

// modelFetchUserAgentEnv 是自定义 User-Agent 的环境变量名。
const modelFetchUserAgentEnv = "AXONHUB_MODEL_FETCH_UA"

// modelFetchUserAgent 在进程启动时读取一次。
//
// 不每次请求都读 os.Getenv 的原因：容器内的环境变量在进程生命周期中不会变化，
// 想改值必然要重启容器，重启就会重新执行这里的初始化。
var modelFetchUserAgent = os.Getenv(modelFetchUserAgentEnv)

// applyModelFetchUserAgent 为「拉取模型列表」的请求写入自定义 User-Agent。
//
// 环境变量留空时直接返回，不写入任何 header。此时 llm/httpclient/client.go
// 中「仅在 User-Agent 缺失时才补默认值」的逻辑会生效，回落到上游原有的
// axonhub/1.0。也就是说：不配置该环境变量时，行为与上游一字不差。
func applyModelFetchUserAgent(headers http.Header) {
	if modelFetchUserAgent == "" {
		return
	}

	headers.Set("User-Agent", modelFetchUserAgent)
}
