import mahler.core.resources


scheduler = mahler.core.resources.build('remoteflow')
print(scheduler.available())

print(scheduler.submit(list(range(100)), container='shallow', tags=['one', 'two', 'three']))
