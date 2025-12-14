// src/core/auth.ts
import { Env } from './config';

export class AuthMiddleware {
  private env: Env;

  constructor(env: Env) {
    this.env = env;
  }

  async verify(request: Request): Promise<boolean> {
    const authHeader = request.headers.get('Authorization');
    
    // 1. 基础检查
    if (!authHeader || !authHeader.startsWith('AWS4-HMAC-SHA256')) return false;

    const credentialMatch = authHeader.match(/Credential=([^/]+)\/([^/]+)\/([^/]+)\/s3\/aws4_request/);
    if (!credentialMatch) return false;

    const [_, accessKeyId, dateStamp, region] = credentialMatch;

    // 2. 校验 AccessKeyID
    if (accessKeyId !== this.env.VIRTUAL_ACCESS_KEY_ID) {
      return false;
    }

    // 3. 检查是否开启严格模式 (Secret Key 是否存在)
    const secretKey = this.env.VIRTUAL_SECRET_ACCESS_KEY;
    if (!secretKey || secretKey.trim() === '') {
      // 宽松模式：不校验签名，直接通过
      return true;
    }

    // 4. 严格模式：校验签名
    try {
      return await this.verifySignature(request, authHeader, secretKey, dateStamp, region);
    } catch (e) {
      console.error('Signature verification failed:', e);
      return false;
    }
  }

  // === AWS V4 签名校验核心逻辑 ===
  private async verifySignature(
    request: Request, 
    authHeader: string, 
    secretKey: string, 
    dateStamp: string,
    region: string
  ): Promise<boolean> {
    const url = new URL(request.url);
    const method = request.method;
    const datetime = request.headers.get('x-amz-date') || request.headers.get('date') || new Date().toISOString();
    
    // 解析 Authorization 头中的各个部分
    const signatureMatch = authHeader.match(/Signature=([a-f0-9]+)/);
    const signedHeadersMatch = authHeader.match(/SignedHeaders=([^,]+)/);
    
    if (!signatureMatch || !signedHeadersMatch) return false;
    
    const clientSignature = signatureMatch[1];
    const signedHeaders = signedHeadersMatch[1];

    // 步骤 1: 创建 Canonical Request
    const canonicalUri = encodeURI(url.pathname); // 需要根据 S3 规则处理 path
    
    // 处理 Query String
    const canonicalQueryString = Array.from(url.searchParams)
      .sort(([k1], [k2]) => k1.localeCompare(k2))
      .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
      .join('&');

    // 处理 Headers
    const headersToSign = signedHeaders.split(';').map(h => h.trim().toLowerCase());
    const canonicalHeaders = headersToSign.map(h => {
      let value = request.headers.get(h) || '';
      // 规范化 header value: 去除多余空格
      value = value.replace(/\s+/g, ' ').trim();
      return `${h}:${value}\n`;
    }).join('');

    // 处理 Payload Hash
    // 注意：如果是流式上传，客户端通常发送 'UNSIGNED-PAYLOAD' 或实际的 SHA256
    // 为了简化并支持流式，如果头里有 x-amz-content-sha256 我们就用它，否则假设 UNSIGNED-PAYLOAD
    const payloadHash = request.headers.get('x-amz-content-sha256') || 'UNSIGNED-PAYLOAD';

    const canonicalRequest = [
      method,
      canonicalUri,
      canonicalQueryString,
      canonicalHeaders,
      signedHeaders,
      payloadHash
    ].join('\n');

    // 步骤 2: 创建 String To Sign
    const credentialScope = `${dateStamp}/${region}/s3/aws4_request`;
    const algorithm = 'AWS4-HMAC-SHA256';
    const canonicalRequestHash = await this.sha256(canonicalRequest);
    
    const stringToSign = [
      algorithm,
      datetime,
      credentialScope,
      canonicalRequestHash
    ].join('\n');

    // 步骤 3: 计算签名 Key
    const kDate = await this.hmac(`AWS4${secretKey}`, dateStamp);
    const kRegion = await this.hmac(kDate, region);
    const kService = await this.hmac(kRegion, 's3');
    const kSigning = await this.hmac(kService, 'aws4_request');

    // 步骤 4: 计算最终签名
    const calculatedSignature = await this.hmacHex(kSigning, stringToSign);

    // 步骤 5: 比对
    return calculatedSignature === clientSignature;
  }

  // --- Crypto 辅助函数 (基于 Web Crypto API) ---

  private async hmac(key: string | ArrayBuffer, data: string): Promise<ArrayBuffer> {
    const enc = new TextEncoder();
    const keyData = typeof key === 'string' ? enc.encode(key) : key;
    
    const cryptoKey = await crypto.subtle.importKey(
      'raw', 
      keyData, 
      { name: 'HMAC', hash: 'SHA-256' }, 
      false, 
      ['sign']
    );
    
    return await crypto.subtle.sign('HMAC', cryptoKey, enc.encode(data));
  }

  private async hmacHex(key: string | ArrayBuffer, data: string): Promise<string> {
    const signatureBuffer = await this.hmac(key, data);
    return this.bufferToHex(signatureBuffer);
  }

  private async sha256(message: string): Promise<string> {
    const enc = new TextEncoder();
    const hashBuffer = await crypto.subtle.digest('SHA-256', enc.encode(message));
    return this.bufferToHex(hashBuffer);
  }

  private bufferToHex(buffer: ArrayBuffer): string {
    return Array.from(new Uint8Array(buffer))
      .map(b => b.toString(16).padStart(2, '0'))
      .join('');
  }
}