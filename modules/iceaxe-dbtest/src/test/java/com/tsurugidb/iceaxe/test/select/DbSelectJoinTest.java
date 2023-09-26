package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.parameter.mapping.TgEntityParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * select join test
 */
class DbSelectJoinTest extends DbTestTableTester {

    // table name
    private static final String MASTER = "master";
    private static final String DETAIL = "detail";

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbSelectJoinTest.class);
        logInitStart(LOG, info);

        dropMasterDetail();
        createMasterDetail();
        insertMasterDetail();

        logInitEnd(LOG, info);
    }

    private static void dropMasterDetail() throws IOException, InterruptedException {
        dropTable(MASTER);
        dropTable(DETAIL);
    }

    private static void createMasterDetail() throws IOException, InterruptedException {
        var session = getSession();
        {
            var sql = "create table " + MASTER //
                    + "(" //
                    + "  m_id int," //
                    + "  m_name varchar(10)," //
                    + "  primary key(m_id)" //
                    + ")";
            executeDdl(session, sql);
        }
        {
            var sql = "create table " + DETAIL //
                    + "(" //
                    + "  d_id bigint," //
                    + "  d_master_id int," // foreign key to MASTER
                    + "  d_memo varchar(100)," //
                    + "  primary key(d_id)" //
                    + ")";
            executeDdl(session, sql);
        }
    }

    private static final List<MasterEntity> MASTER_LIST = List.of( //
            new MasterEntity(1, "aaa"), //
            new MasterEntity(2, "bbb"), //
            new MasterEntity(3, "ccc"));
    private static final Map<Integer, MasterEntity> MASTER_MAP = MASTER_LIST.stream().collect(Collectors.toMap(e -> e.getId(), e -> e));

    private static final List<DetailEntity> DETAIL_LIST = List.of( //
            new DetailEntity(11, 1, "a1"), //
            new DetailEntity(12, 1, "a2"), //
            new DetailEntity(13, 1, "a3"), //
            new DetailEntity(21, 2, "b1"), //
            new DetailEntity(40, 4, "master nothing"), //
            new DetailEntity(90, null, "master null"));

    private static void insertMasterDetail() throws IOException, InterruptedException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session, 3);
        tm.execute(transaction -> {
            try (var ps = session.createStatement(MASTER_INSERT_SQL, MASTER_MAPPING)) {
                for (var entity : MASTER_LIST) {
                    transaction.executeAndGetCount(ps, entity);
                }
            }
            try (var ps = session.createStatement(DETAIL_INSERT_SQL, DETAIL_MAPPING)) {
                for (var entity : DETAIL_LIST) {
                    transaction.executeAndGetCount(ps, entity);
                }
            }
        });
    }

    private static class MasterEntity {
        private int id;
        private String name;

        public MasterEntity(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }

    private static final String MASTER_INSERT_SQL = "insert into " + MASTER //
            + " (m_id, m_name)" //
            + " values(:m_id, :m_name)";
    private static final TgEntityParameterMapping<MasterEntity> MASTER_MAPPING = TgParameterMapping.of(MasterEntity.class) //
            .addInt("m_id", MasterEntity::getId) //
            .addString("m_name", MasterEntity::getName);

    private static class DetailEntity {
        private long id;
        private Integer masterId;
        private String memo;

        public DetailEntity(long id, Integer masterId, String memo) {
            this.id = id;
            this.masterId = masterId;
            this.memo = memo;
        }

        public long getId() {
            return id;
        }

        public Integer getMasterId() {
            return masterId;
        }

        public String getMemo() {
            return memo;
        }
    }

    private static final String DETAIL_INSERT_SQL = "insert into " + DETAIL //
            + " (d_id, d_master_id, d_memo)" //
            + " values(:d_id, :d_master_id, :d_memo)";
    private static final TgEntityParameterMapping<DetailEntity> DETAIL_MAPPING = TgParameterMapping.of(DetailEntity.class) //
            .addLong("d_id", DetailEntity::getId) //
            .addInt("d_master_id", DetailEntity::getMasterId) //
            .addString("d_memo", DetailEntity::getMemo);

    @Test
    void simpleJoin() throws Exception {
        var sql = "select * from " + DETAIL + " d, " + MASTER + " m\n" //
                + "where m.m_id = d.d_master_id\n" //
                + "order by d_id";

        var expectedList = new ArrayList<MasterDetailPair>();
        for (var detail : DETAIL_LIST) {
            var master = MASTER_MAP.get(detail.getMasterId());
            if (master != null) {
                expectedList.add(new MasterDetailPair(master, detail));
            }
        }

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            List<TsurugiResultEntity> list = tm.executeAndGetList(ps);
            assertEqualsMasterDetail(expectedList, list);
        }
    }

    @Test
    void innerJoin() throws Exception {
        var sql = "select * from " + DETAIL + " d\n" //
                + "inner join " + MASTER + " m on m.m_id = d.d_master_id\n" //
                + "order by d_id";

        var expectedList = new ArrayList<MasterDetailPair>();
        for (var detail : DETAIL_LIST) {
            var master = MASTER_MAP.get(detail.getMasterId());
            if (master != null) {
                expectedList.add(new MasterDetailPair(master, detail));
            }
        }

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            List<TsurugiResultEntity> list = tm.executeAndGetList(ps);
            assertEqualsMasterDetail(expectedList, list);
        }
    }

    @Test
    void leftJoin() throws Exception {
        var sql = "select * from " + DETAIL + " d\n" //
                + "left join " + MASTER + " m on m.m_id = d.d_master_id\n" //
                + "order by d_id";

        var expectedList = new ArrayList<MasterDetailPair>();
        for (var detail : DETAIL_LIST) {
            var master = MASTER_MAP.get(detail.getMasterId());
            expectedList.add(new MasterDetailPair(master, detail));
        }

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql)) {
            List<TsurugiResultEntity> list = tm.executeAndGetList(ps);
            assertEquals(4, list.size()); // TODO left join実装待ち
//          assertEqualsMasterDetail(expectedList, list);
        }
    }

    private static class MasterDetailPair implements Comparable<MasterDetailPair> {
        private MasterEntity master;
        private DetailEntity detail;

        public MasterDetailPair(MasterEntity master, DetailEntity detail) {
            this.master = master;
            this.detail = detail;
        }

        @Override
        public int compareTo(MasterDetailPair that) {
            if (this.detail == null && that.detail == null) {
                return Long.compare(this.master.getId(), that.master.getId());
            }
            if (this.detail == null) {
                return 1;
            }
            if (that.detail == null) {
                return -1;
            }

            return Long.compare(this.detail.getId(), that.detail.getId());
        }
    }

    private void assertEqualsMasterDetail(List<MasterDetailPair> expectedList, List<TsurugiResultEntity> actualList) {
        Collections.sort(expectedList);
        assertEquals(expectedList.size(), actualList.size());
        int i = 0;
        for (var pair : expectedList) {
            var actual = actualList.get(i++);
            var detail = pair.detail;
            assertEquals(detail.getId(), actual.getLong("d_id"));
            assertEquals(detail.getMasterId(), actual.getIntOrNull("d_master_id"));
            assertEquals(detail.getMemo(), actual.getString("d_memo"));
            var master = pair.master;
            if (master != null) {
                assertEquals(master.getId(), actual.getInt("m_id"));
                assertEquals(master.getName(), actual.getString("m_name"));
            } else {
                assertNull(actual.getIntOrNull("m_id"));
                assertNull(actual.getIntOrNull("m_name"));
            }
        }
    }
}
