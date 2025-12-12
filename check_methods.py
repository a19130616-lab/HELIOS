from binance.um_futures import UMFutures
import inspect

print("Methods in UMFutures:")
methods = [m[0] for m in inspect.getmembers(UMFutures, predicate=inspect.isfunction)]
for m in methods:
    if 'trade' in m.lower():
        print(m)
