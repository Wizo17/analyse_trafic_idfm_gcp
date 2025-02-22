@echo off
echo Creating the virtual environment...
python -m venv venv

echo Environment activation...
call venv\Scripts\activate

echo Installing dependencies...
pip install -r requirements.txt

echo Environment successfully configured!
pause
