import time 
start_time = time.time()

time.sleep(3)

end_time = time.time()


duration = end_time - start_time

rounded = round(duration)
print(rounded)