@ECHO OFF
TITLE Data Processing Pipeline

REM --- Configuration ---
SET PYTHON_EXE=python
SET MAIN_SCRIPT=main.py
SET LOGFILE=debug_log.txt

REM --- Start of Script ---
CLS
ECHO.
ECHO ⚙️  Starting the data processing pipeline...
ECHO    Running %MAIN_SCRIPT%
ECHO --------------------------------------------------

REM Clear the previous log file
IF EXIST "%LOGFILE%" ( DEL "%LOGFILE%" )

REM Run the main Python wrapper and redirect any errors to the log file
%PYTHON_EXE% "%MAIN_SCRIPT%" 2>> "%LOGFILE%"

REM Check the final exit code of the Python script
IF %ERRORLEVEL% NEQ 0 (
    ECHO.
    ECHO ❌ ERROR: The pipeline failed.
    ECHO    Please check the "%LOGFILE%" file for details.
    ECHO --------------------------------------------------
) ELSE (
    ECHO.
    ECHO ✅ Pipeline completed.
    ECHO --------------------------------------------------
)

PAUSE