BEGIN TRANSACTION;
CREATE TABLE "HKCLASSES" ( "type" SMALLINT NOT NULL  , "name" CHAR(190)  NOT NULL  , "value" MEMO NOT NULL  , "user" CHAR(50)  , "update" DATETIME, PRIMARY KEY ( "type" , "name" ) );
CREATE TABLE "TB_NODE" ( "NOD_ID" INTEGER PRIMARY KEY , "NOD_HOSTNAME" CHAR(50)  NOT NULL  , "NOD_ENABLED" BOOL);
CREATE TABLE "TB_SLOT" ( "SL_ID" INTEGER PRIMARY KEY , "SL_CAPACITY" DOUBLE NOT NULL  , "NOD_ID" INTEGER NOT NULL  , "SLT_ID" INTEGER );
CREATE TABLE "TB_SLOT_TYPE" ( "SLT_ID" INTEGER PRIMARY KEY , "SLT_NAME" CHAR(50)  NOT NULL);
CREATE TABLE "TB_RESERVATION" ( "RES_ID" INTEGER PRIMARY KEY , "RES_NAME" CHAR(50), "RES_STATUS" SMALLINT NOT NULL);
CREATE TABLE "TB_RESPART" ( "RSP_ID" INTEGER PRIMARY KEY , "RSP_NAME" CHAR(50)  , "RES_ID" INTEGER NOT NULL  , "RSPT_ID" INTEGER, "RSP_STATUS" SMALLINT NOT NULL,  "RSP_PREEMPTIBLE" BOOL NOT NULL);
CREATE TABLE "TB_RESPART_TYPE" ( "RSPT_ID" INTEGER PRIMARY KEY , "RSPT_NAME" CHAR(50) );
CREATE TABLE "TB_ALLOC" ( "SL_ID" INTEGER NOT NULL  , "RSP_ID" INTEGER NOT NULL  , "ALL_SCHEDSTART" DATETIME NOT NULL  , "ALL_SCHEDEND" DATETIME NOT NULL , "ALL_REALEND" DATETIME , "ALL_AMOUNT" DOUBLE NOT NULL  , "ALL_MOVEABLE" BOOL NOT NULL  , "ALL_DURATION" INTEGER , "ALL_REALDURATION" INTEGER, "ALL_DEADLINE" DATETIME , "ALL_STATUS" SMALLINT NOT NULL, "ALL_NEXTSTART" DATETIME, PRIMARY KEY ( "SL_ID" , "RSP_ID" , "ALL_SCHEDSTART" ) );
CREATE VIEW V_ALLOCATION as select * from tb_reservation r inner join tb_respart rp on r.res_id = rp.res_id inner join tb_alloc a on a.rsp_id = rp.rsp_id;
CREATE VIEW V_ALLOCSLOT as select * from tb_slot s left join tb_alloc a on a.sl_id = s.sl_id left join tb_respart rp on a.rsp_id = rp.rsp_id;
CREATE INDEX "IDX_ALLOC_START" ON "TB_ALLOC" ( "ALL_SCHEDSTART" );
CREATE INDEX "IDX_ALLOC_END" ON "TB_ALLOC" ( "ALL_SCHEDEND" );
CREATE INDEX "IDX_ALLOC_ENDREAL" ON "TB_ALLOC" ( "ALL_REALEND" );
CREATE INDEX "IDX_ALLOC_SLID" ON "TB_ALLOC" ( "SL_ID" );
CREATE INDEX "IDX_ALLOC_RSPID" ON "TB_ALLOC" ( "RSP_ID" );
CREATE INDEX "IDX_SLOT_NODID" ON "TB_SLOT" ( "NOD_ID" );
COMMIT;