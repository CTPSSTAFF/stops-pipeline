@ECHO OFF
TITLE Data Processing Pipeline

:START_PIPELINE
REM --- Configuration ---
SET PYTHON_EXE=python
SET MAIN_SCRIPT=main.py
SET LOGFILE=debug_log.txt

REM --- Start of Script ---
CLS
ECHO.
ECHO   Starting the data processing pipeline...
ECHO   Running %MAIN_SCRIPT%
ECHO --------------------------------------------------

REM Clear the previous log file
IF EXIST "%LOGFILE%" ( DEL "%LOGFILE%" )

REM Run the main Python wrapper and redirect any errors to the log file
%PYTHON_EXE% "%MAIN_SCRIPT%" 2>> "%LOGFILE%"

REM Check the final exit code of the Python script
IF %ERRORLEVEL% NEQ 0 (
    ECHO.
    ECHO ❌ ERROR: The pipeline failed.
    ECHO   Please check the "%LOGFILE%" file for details.
    ECHO --------------------------------------------------
) ELSE (
    ECHO.
    ECHO ✅ Pipeline completed.
    ECHO --------------------------------------------------
)

@REM Uncomment the following line to pause the script before the rerun prompt
REM PAUSE
REM Keep only above statement if user should press any key to close the window
REM below allows for ease of re-run for development purposes

REM --- Rerun Prompt ---
ECHO.
CHOICE /C RC /N /M "Press [R] to re-run, or [C] to close the window."

REM Check the ERRORLEVEL set by the CHOICE command.
REM ERRORLEVEL is 1 for the first choice (R), 2 for the second (C).
IF ERRORLEVEL 2 GOTO :EOF
IF ERRORLEVEL 1 GOTO :START_PIPELINE