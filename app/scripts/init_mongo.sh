#!/bin/bash

# 4. MongoDB 초기화 작업 시작
echo "
====================================
Executing MongoDB Initialization
====================================
"

# MongoDB 초기화 명령어를 실행 (실행을 위해 /bin/bash 사용)
docker exec -i -t mongodb /bin/bash -c "mongosh -u rootuser -p rootpass <<EOF
use test

if (db.getUser(\"testuser\") == null) {
    db.createUser({
        \"user\": \"testuser\",
        \"pwd\": \"testpass\",
        \"roles\": [{\"role\": \"readWrite\", \"db\": \"test\"}]
    });
    print(\"User 'testuser' created successfully.\");
} else {
    print(\"User 'testuser' already exists.\");
}

if (db.getCollectionNames().indexOf(\"passengers\") === -1) {
    db.createCollection(\"passengers\");
    print(\"Collection 'passengers' created successfully.\");
} else {
    print(\"Collection 'passengers' already exists.\");
}
EOF"

# 7. MongoDB 초기화 완료 확인
if [ $? -eq 0 ]; then
    echo "
====================================
MongoDB Initialization Complete
====================================
"
else
    echo "
====================================
MongoDB Initialization Failed
====================================
"
    exit 1
fi

# 8. 이후 프로세스 실행
exec "$@"
