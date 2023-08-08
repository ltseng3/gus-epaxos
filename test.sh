timeout 180s bin/client -maddr=10.10.1.1 -writes=0.01 -c=0 -T=450
python3 client_metrics.py
