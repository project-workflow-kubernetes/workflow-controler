import docker
import redis
from workflow import settings

def get_docker_id(docker_image):
  client = docker.from_env()
  image = client.images.get(docker_image)
  return image.id

def has_changed(docker_image):
  r = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0)
  if r.get(docker_image):
    return r.get(docker_image) != get_docker_id(docker_image).encode()
  # If not existen in redis, for sure it has been changed
  return True

def set_docker_id(docker_image):
  r = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0)
  r.set(docker_image, get_docker_id(docker_image).encode())

def clear_all():
  r = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0)
  for key in r.keys():
    r.delete(key)

#clear_all()
#print(get_docker_id('rusucosmin/spark:latest'))
#print(has_changed('rusucosmin/spark:latest'))
#print(set_docker_id('rusucosmin/spark:latest'))
#print(has_changed('rusucosmin/spark:latest'))


