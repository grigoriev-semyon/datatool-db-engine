import uvicorn
from dbengine import app

if __name__ == '__main__':
    uvicorn.run(app)
