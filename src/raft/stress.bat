@echo off

REM 设置循环次数
set NUM_RUNS=%1

REM 设置要运行的测试命令
set TEST_COMMAND=%2

REM 初始化计数器和失败标志
set counter=0
set fail_found=false

REM 清空控制台
cls

REM 检查命令行参数
if "%NUM_RUNS%"=="" (
  echo 请提供循环次数作为参数！
  exit /b 1
)

if "%TEST_COMMAND%"=="" (
  echo 请提供要运行的测试命令作为参数！
  exit /b 1
)

REM 创建测试日志文件
echo. > test_log.txt

REM 循环运行测试程序
for /L %%i in (1, 1, %NUM_RUNS%) do (
  REM 运行测试命令并将输出存储到临时文件
  go test -run %TEST_COMMAND% -race > temp_log.txt 2>&1

  REM 检查临时文件中是否包含 "FAIL" 字符串
  findstr /c:"--- FAIL" temp_log.txt >nul
  if %errorlevel% equ 0 (
    set fail_found=true

    REM 将临时文件的内容追加到日志文件
    type temp_log.txt >> test_log.txt

    REM 删除临时文件
    del temp_log.txt

    goto :BreakLoop
  )

  REM 更新计数器
  set /a counter=counter+1

  REM 清空控制台并打印进度
  cls
  echo Progress: %counter% / %NUM_RUNS%
)

:BreakLoop

REM 输出结果
if %fail_found%==true (
  echo FAIL
) else (
  echo All tests passed.
)
