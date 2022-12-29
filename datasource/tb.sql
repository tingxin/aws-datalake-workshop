CREATE TABLE IF NOT EXISTS `order` (
    order_id INT AUTO_INCREMENT NOT NULL,
    goods_id INT NOT NULL,
    user_mail varchar(20) NOT NULL,
    status char(10) NOT NULL, 
    good_count INT NOT NULL,
    amount FLOAT NOT NULL,
    create_time datetime NOT NULL,
    update_time datetime NOT NULL,
    PRIMARY KEY (`order_id`)
);

CREATE TABLE `user` (
    user_mail varchar(20) NOT NULL,
    city varchar(20) NOT NULL,
    sex char(10) NOT NULL
)

INSERT INTO `user` VALUES("barry.xu@163.com","shanghai","man");
INSERT INTO `user` VALUES("dandan@qq.com","shanghai","female");
INSERT INTO `user` VALUES("pony@qq.com","shengzhen","man");
INSERT INTO `user` VALUES("focus@qq.com","beijing","female");

CREATE TABLE `goods` (
    goods_id INT,
    sku varchar(20) NOT NULL,
    spu  varchar(20) NOT NULL
)

INSERT INTO `goods` VALUES(0,"boy shirt","shirt");
INSERT INTO `goods` VALUES(1,"girl shirt","shirt");
INSERT INTO `goods` VALUES(2,"boy shoes","shoes");
INSERT INTO `goods` VALUES(3,"girl shoes","shirt");
INSERT INTO `goods` VALUES(4,"boy pants","pants");
INSERT INTO `goods` VALUES(5,"girl pants","pants");
INSERT INTO `goods` VALUES(6,"boy coat","coat");
INSERT INTO `goods` VALUES(7,"girl coat","coat");
INSERT INTO `goods` VALUES(8,"girl skirt","skirt");



SELECT o.*,u.sex, u.city,g.sku, g.spu
FROM `order` o
LEFT JOIN `user` u
ON o.user_mail = u.user_mail
LEFT JOIN `goods` g
ON o.goods_id = g.goods_id
ORDER BY o.create_time DESC
LIMIT 10;


CREATE TABLE `simple_event` (
    id BIGINT NOT NULL,
     name STRING,
     description STRING,
     weight DECIMAL(10,3),
    PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',      -- 可选 'mysql-cdc' 和 'postgres-cdc'
    'hostname' = 'demo.c6lwjjfhbm6a.rds.cn-northwest-1.amazonaws.com.cn',  
                                    -- 数据库的 IP
    'port' = '3306',                -- 数据库的访问端口
    'username' = 'admin',        -- 数据库访问的用户名（需要提供 SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, SELECT, RELOAD 权限）
    'password' = 'Demo1234',    -- 数据库访问的密码
    'database-name' = 'demo',   -- 需要同步的数据库
    'table-name' = 'simple2'      -- 需要同步的数据表名
);
 update `order` set user_mail = "barry.xu@163.com" where order_id =8594;
 update `order` set user_mail = "pony@qq.com" where order_id =8594;

 INSERT INTO `order` ( user_mail,goods_id,status,good_count,amount,create_time,update_time ) VALUES( 'pony@qq.com',5,'unpaid',1,505.0,'2022-05-17 11:15:34','2022-05-17 11:15:34' )