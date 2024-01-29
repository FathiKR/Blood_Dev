#!/bin/bash

@echo off 
set today=%Year%-%Month%-%Day%
echo ### Script is running on: %today% ###\
cd /d C:\Work\Project\Dev\
cd /d .env\Scripts
Activate.ps1
cd /d ..
cd /d ..
echo %CD%
echo ### At source directory ###
python master_data_processing.py
python notification.py
git pull
git add .
git commit -m "Update master data!"
git push
echo ####Push Telegram Notification######
