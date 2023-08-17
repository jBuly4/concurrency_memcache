#  Memcache

## Task
Create more effective script from one-threaded memc_load.py

### What is done
- added multiprocessing:
  - shared values and lock for them through Manager. But lock is not used
  - opening files through processes
- parsing files through ThreadPoolExecutor
  - using batch
  - using pool of tasks

## Run
- clone this repo
- use commands:
```bash
Usage: memc_load.py [options]

Options:
  -h, --help         show this help message and exit
  -t, --test         
  -l LOG, --log=LOG  
  --dry              
  --pattern=PATTERN  
  --idfa=IDFA        
  --gaid=GAID        
  --adid=ADID        
  --dvid=DVID        
```
- run with defaults
```bash
python memc_load.py
```



