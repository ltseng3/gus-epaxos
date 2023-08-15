timeout 180s bin/client -maddr=10.10.1.1 -writes=0.505 -c=25up -T=80
python3 client_metrics.py
