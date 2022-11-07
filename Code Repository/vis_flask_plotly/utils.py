def logRequest(path, method, pr=False, data=None, received=True):

  direction = 'received at' if received else 'sent to'

  if pr:
    print (f'{method} {direction} {path}{" with data:" if data else ""}')
    if data:
      print (data)
  else:
    return f'{method} {direction} at {path}'
  
def logData(name="", data=None):
  print (f'Current {name} data:')
  print (data)