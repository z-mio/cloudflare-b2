import { AwsClient } from 'aws4fetch'
const UNSIGNABLE_HEADERS = [
  'x-forwarded-proto',
  'x-real-ip',
  'accept-encoding',
  'if-match',
  'if-modified-since',
  'if-none-match',
  'if-range',
  'if-unmodified-since',
]
const HTTPS_PROTOCOL = 'https:'
const HTTPS_PORT = '443'
const RANGE_RETRY_ATTEMPTS = 3

/**
 * 工具函数
 */
function filterHeaders(headers, env) {
  // noinspection JSCheckFunctionSignatures
  return new Headers(
    Array.from(headers.entries()).filter(
      (pair) =>
        !(
          UNSIGNABLE_HEADERS.includes(pair[0]) ||
          pair[0].startsWith('cf-') ||
          ('ALLOWED_HEADERS' in env &&
            !env['ALLOWED_HEADERS'].includes(pair[0]))
        ),
    ),
  )
}

function createHeadResponse(response) {
  // 对于 HEAD 请求，仅返回响应头与状态码（无响应体）
  return new Response(null, {
    headers: response.headers,
    status: response.status,
    statusText: response.statusText,
  })
}

function isListBucketRequest(env, path) {
  const pathSegments = path.split('/')
  return (
    (env['BUCKET_NAME'] === '$path' && pathSegments.length < 2) ||
    (env['BUCKET_NAME'] !== '$path' && path.length === 0)
  )
}

function stripPreSignedParams(url) {
  // 移除所有预签名相关的查询参数（X-Amz-*），避免与 Worker 的二次签名冲突
  const keysToDelete = []
  for (const [key] of url.searchParams) {
    if (key.toLowerCase().startsWith('x-amz-')) {
      keysToDelete.push(key)
    }
  }
  for (const key of keysToDelete) {
    url.searchParams.delete(key)
  }
}

/**
 * Worker 入口
 */
// noinspection JSUnusedGlobalSymbols
export default {
  async fetch(request, env) {
    // 仅允许 GET 和 HEAD 方法
    if (!['GET', 'HEAD'].includes(request.method)) {
      return new Response(null, {
        status: 405,
        statusText: 'Method Not Allowed',
      })
    }

    const url = new URL(request.url)
    url.protocol = HTTPS_PROTOCOL
    url.port = HTTPS_PORT

    // 移除任何预签名的 S3 查询参数，避免与本次签名冲突
    stripPreSignedParams(url)

    // 规范化路径（去除前后斜杠）
    let path = url.pathname.replace(/^\//, '')
    path = path.replace(/\/$/, '')

    // 禁止列出桶内容，除非显式开启
    if (
      isListBucketRequest(env, path) &&
      String(env['ALLOW_LIST_BUCKET']) !== 'true'
    ) {
      return new Response(null, { status: 404, statusText: 'Not Found' })
    }

    const rcloneDownload = String(env['RCLONE_DOWNLOAD']) === 'true'

    // 根据桶映射模式解析目标主机名
    switch (env['BUCKET_NAME']) {
      case '$path':
        url.hostname = env['B2_ENDPOINT']
        break
      case '$host':
        url.hostname = url.hostname.split('.')[0] + '.' + env['B2_ENDPOINT']
        break
      default:
        url.hostname = env['BUCKET_NAME'] + '.' + env['B2_ENDPOINT']
        break
    }

    const headers = filterHeaders(request.headers, env)

    const client = new AwsClient({
      accessKeyId: env['B2_APPLICATION_KEY_ID'],
      secretAccessKey: env['B2_APPLICATION_KEY'],
      service: 's3',
    })

    const requestMethod = request.method

    // 可选：rclone 下载场景下，移除路径中的 "file/" 前缀
    if (rcloneDownload) {
      if (env['BUCKET_NAME'] === '$path') {
        // 从路径中删除前导文件/前缀
        url.pathname = path.replace(/^file\//, '')
      } else {
        // 从路径中删除前导的 file/{bucket_name}/ 前缀
        url.pathname = path.replace(/^file\/[^/]+\//, '')
      }
    }

    const signedRequest = await client.sign(url.toString(), {
      method: 'GET',
      headers: headers,
    })

    // 针对 Range 请求的特殊处理：若响应缺少 content-range，重试数次
    if (signedRequest.headers.has('range')) {
      let attempts = RANGE_RETRY_ATTEMPTS
      let response
      do {
        const controller = new AbortController()
        response = await fetch(signedRequest.url, {
          method: signedRequest.method,
          headers: signedRequest.headers,
          signal: controller.signal,
        })
        if (response.headers.has('content-range')) {
          if (attempts < RANGE_RETRY_ATTEMPTS) {
            console.log(
              `Retry for ${signedRequest.url} succeeded - response has content-range header`,
            )
          }
          break
        } else if (response.ok) {
          attempts -= 1
          console.error(
            `Range header in request for ${signedRequest.url} but no content-range header in response. Will retry ${attempts} more times`,
          )
          if (attempts > 0) {
            controller.abort()
          }
        } else {
          break
        }
      } while (attempts > 0)

      if (attempts <= 0) {
        console.error(
          `Tried range request for ${signedRequest.url} ${RANGE_RETRY_ATTEMPTS} times, but no content-range in response.`,
        )
      }

      if (requestMethod === 'HEAD') {
        return createHeadResponse(response)
      }

      return response
    }

    const fetchPromise = fetch(signedRequest)

    if (requestMethod === 'HEAD') {
      const response = await fetchPromise
      return createHeadResponse(response)
    }

    return fetchPromise
  },
}
