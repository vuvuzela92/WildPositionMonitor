#!/bin/bash
cd /home/vector/APPS/WildPositionMonitor
/home/vector/APPS/WildPositionMonitor/.venv/bin/python -m src.main >> /home/vector/APPS/WildPositionMonitor/logs/cron_output.log 2>&1
