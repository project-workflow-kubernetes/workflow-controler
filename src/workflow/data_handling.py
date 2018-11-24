from minio import Minio

import settings as s


minioPersistent = Minio(s.PERSISTENT_ADDR,
                        access_key=s.ACCESS_KEY,
                        secret_key=s.SECRET_KEY,
                        secure=False)

minioTemporary = Minio(s.TEMPORARY_ADDR,
                       access_key=s.ACCESS_KEY,
                       secret_key=s.SECRET_KEY,
                       secure=False)


if __name__ == '__main__':
    print(minioPersistent.list_buckets())
    print(minioTemporary.list_buckets())
