regular = {
    'monday': [{'open': '00:00', 'close': '23:59'}],
    'tuesday': [{'open': '00:00', 'close': '23:59'}],
    'wednesday': [{'open': '00:00', 'close': '23:59'}],
    'thursday': [{'open': '00:00', 'close': '23:59'}],
    'friday': [{'open': '00:00', 'close': '23:59'}],
    'saturday': [{'open': '00:00', 'close': '23:59'}],
    'sunday': [{'open': '00:00', 'close': '23:59'}],
    'exceptionDays': [
        {'from': '2025-04-18T00:00:00', 'times': [{'open': '00:00', 'close': '23:59'}]},
        {'from': '2025-04-21T00:00:00', 'times': [{'open': '00:00', 'close': '23:59'}]},
        ...
    ]
}
for den, seznam in regular.items(): 
    zaznam = seznam[0]
    for stav, cas in zaznam.items():
  
        print(den, stav, cas)
#   