#!/bin/bash

@echo off 
set today=%Year%-%Month%-%Day%
echo ### Script is running on: %today% ###
cd /d C:\Work\Project\Dev\
echo %CD%
echo ### At source directory ###
python master_data_processing.py