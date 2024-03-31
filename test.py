import asyncio
import time
import loop
from asyncio import ProactorEventLoop

from utils import timer

@timer.atimer
async def test_write_file():
    import aiofile
    async with aiofile.open("write1.txt", "wb") as fp:
        # write_tasks = []
        cost_sum = 0
        for i in range(10 ** 6):
            s = f"{i}" * 100
            # @timer.atimer
            # async def _write(fp, data):
                # await fp.write(data)
            start = time.time_ns()
            # await _write(fp, f"中{i}:{s}\n")
            await fp.write(f"中{i}:{s}\n".encode("gbk"))
            end = time.time_ns()
            cost_sum += end - start
        print(f"cost_sum:{cost_sum}")
        
@timer.timer
def sync_write_file():
    with open("write2.txt", "wb") as fp:
        cost_sum = 0
        for i in range(10 ** 6):
            s = f"{i}" * 100
            start = time.time_ns()
            # @timer.timer
            def _write(fp, data):
                fp.write(data)
            _write(fp, f"中{i}:{s}\n".encode("gbk"))
            end = time.time_ns()
            cost_sum += end - start
        print(f"sync_write_file cost_sum:{cost_sum/1000/1000}")

@timer.atimer
async def main():
    await test_write_file()

def validate_result():
    with open("write1.txt", "rb") as fp1, open("write2.txt", "rb") as fp2:
        print("validate_result", fp1.read() == fp2.read())

iocp = loop.IocpProactor()
lp = loop.ProactorEventLoop(iocp)
lp.run_until_complete(main())
sync_write_file()
validate_result()