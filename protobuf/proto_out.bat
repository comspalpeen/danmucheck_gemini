@echo off
:: 自动切换到脚本所在目录
cd /d "%~dp0"

echo ============= 开始编译 proto 文件 =============
echo  执行命令：protoc --python_out=. douyin.proto
echo ==============================================

:: 直接执行你手动输入的正确命令（无任何变量/注释干扰）
protoc.exe --python_out=. douyin.proto

:: 结果判断
if %errorlevel% equ 0 (
    echo ? 编译成功！
    echo  生成的 Python 文件（douyin_pb2.py）已保存到当前目录
) else (
    echo ? 编译失败！仅可能原因：
    echo  ① douyin.proto 文件存在语法错误（请检查其内容格式）
)

pause