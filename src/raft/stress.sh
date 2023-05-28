#!/bin/bash

# 从命令行参数获取循环次数
NUM_RUNS=$1

# 从命令行参数获取执行命令版本
TEST_COMMAND=$2

# 初始化计数器和失败标志
counter=0
fail_found=false

# 清空控制台
clear

# 检查命令行参数
if [[ -z $NUM_RUNS || -z $TEST_COMMAND ]]; then
  echo "请提供循环次数和执行命令版本作为参数！"
  exit 1
fi
rm -rf test_log
go env -w GOPROXY=https://goproxy.cn,direct
# 循环运行测试程序
for ((i=1; i<=NUM_RUNS; i++)); do
  # 运行测试程序
  output=$(go test -run $TEST_COMMAND -race 2>&1)
  echo "${output}" >> test_log
  # 检查输出中是否包含 "FAIL" 字符串
  if [[ $output == *"FAIL"* ]]; then
    fail_found=true
    break
  fi
  rm -rf test_log
  # 更新计数器
  counter=$((counter + 1))

  # 清空控制台并打印进度
  clear
  echo "Progress: $counter / $NUM_RUNS"
done

# 输出结果
if [ "$fail_found" = true ]; then
  echo "FAIL"
else
  echo "All tests passed."
fi
