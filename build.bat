set SOURCE=src\bot_detector\__main__.py
set OUTPUT_DIR=dist

nuitka ^
    --standalone ^
    --mingw64
    --output-filename=bot_detector.exe ^
    --follow-imports ^
    --include-package=src.bot_detector ^
    --include-data-dir=./models=models ^
    --include-data-file=./settings.ini=settings.ini ^
    --include-data-file=./settings.ini.example=settings.ini.example ^
    --output-dir=%OUTPUT_DIR% ^
    --assume-yes-for-downloads ^
    --windows-console-mode=force ^
    --no-deployment-flag=self-execution ^
    %SOURCE%