# Producer entrypoint script
# Routes to the correct producer based on PRODUCER_TYPE env var

import os
import sys

producer_type = os.getenv("PRODUCER_TYPE", "stock").lower()

if producer_type == "stock":
    from stock_producer import run
elif producer_type == "crypto":
    from crypto_producer import run
else:
    print(f"Unknown PRODUCER_TYPE: {producer_type}")
    sys.exit(1)

if __name__ == "__main__":
    run()
