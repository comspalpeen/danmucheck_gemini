# fix_data.py (放在项目根目录运行一次即可)
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient

async def fix():
    client = AsyncIOMotorClient("mongodb://gogogo:chufale@localhost:4396/")
    db = client["douyin_live_data"]
    col = db["live_gifts"]
    
    print("开始修复数据...")
    async for doc in col.find({"total_diamond_count": {"$exists": False}}):
        # 补算逻辑
        d = doc.get('diamond_count', 0)
        c = doc.get('combo_count', 1)
        g = doc.get('group_count', 1)
        if c <= 0: c = 1
        if g <= 0: g = 1
        total = d * c * g
        
        await col.update_one(
            {"_id": doc["_id"]},
            {"$set": {"total_diamond_count": total}}
        )
    print("修复完成！")

if __name__ == "__main__":
    asyncio.run(fix())