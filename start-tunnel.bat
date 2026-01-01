@echo off
echo ========================================
echo 正在建立 SSH 隧道连接到远程 PostgreSQL
echo ========================================
echo.
echo 请保持此窗口运行，不要关闭！
echo 关闭此窗口将断开数据库连接
echo.
echo 按 Ctrl+C 可以停止隧道
echo ========================================
echo.

ssh -F NONE -i C:\Users\DELL\.ssh\id_ed25519 -p 33252 -L 15432:localhost:5432 root@156.254.126.67 -N

pause

