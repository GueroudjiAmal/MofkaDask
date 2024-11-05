## Non-Blocking Online Mofka-Dask Analytics 

To optimize performance, these scripts allow `producer.push()` operations to be executed asynchronously. For Dask workflows with a high number of tasks, this can significantly decrease overhead. To ensure all data is placed in a partition before a worker exits, submit a task with the key "flush-mofka-buffer" as shown below:


```
def no_op():
    return None
            
client.submit(no_op, key="flush-mofka-buffer")
```