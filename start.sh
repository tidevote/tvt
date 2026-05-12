#!/bin/bash
# TVT RPC 代理启动脚本
cd "$(dirname "$0")"
echo "🌊 启动 TVT 代理服务器..."
node rpc-proxy.js
