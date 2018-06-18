package cami.io;

import cami.io.concat.ConcatProfilingIter;
import mzd.taxonomy.neo.NeoDao;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ConcatReaderTest extends TestResources{

    private static NeoDao neoDao = null;

    @BeforeClass
    public static void openDB() throws Base.ParseException, IOException, InterruptedException {
        neoDao = new NeoDao(new File(DBPATH));
    }

    @AfterClass
    public static void closeDB() throws IOException {
        if (neoDao != null) {
            neoDao.shutdown();
        }
    }

    @Test
    public void testProfileValid() throws Base.ParseException, IOException {

        Profile.ValidatingReader pr = new Profile.ValidatingReader(
                RESOURCE_PATH + "profile-valid.txt", neoDao, true);
        int rc = 0;
        String[] row;
        while ((row = pr.readRow()) != null) {
            assertEquals(6, row.length);
            rc++;
        }
        assertEquals(12, rc);
    }

    @Test
    public void testConcatProfileValid() throws Base.ParseException, IOException {

        ConcatProfilingIter prIter = new ConcatProfilingIter(
                RESOURCE_PATH + "concat-profile-valid.txt", neoDao, true);
        int rc = 0;
        String[] row;
        while ((row = prIter.readRow()) != null) {
            rc++;
        }
        System.out.println(rc);
        assertEquals(24, rc);
    }

    @Test()
    public void canDetectLongConcatProfileVersion() throws Base.ParseException, IOException {

        ConcatProfilingIter prIter = new ConcatProfilingIter(
                RESOURCE_PATH + "concat-profile-valid-long-version.txt", neoDao, true);
        int rc = 0;
        String[] row;
        while ((row = prIter.readRow()) != null) {
            rc++;
        }
        System.out.println(rc);
        assertEquals(24, rc);
    }


    @Test
    public void testConcatProfileMinValid() throws Base.ParseException, IOException {

        ConcatProfilingIter prIter = new ConcatProfilingIter(
                RESOURCE_PATH + "concat-profile-min-valid.txt", neoDao, true);
        int rc = 0;
        String[] row;
        while ((row = prIter.readRow()) != null) {
            rc++;
        }
        System.out.println(rc);
        assertEquals(24, rc);
    }

   // @Test(expected = Base.HeaderException.class)
    public void canDetectMissingSampleId() throws Base.ParseException, IOException {
        ConcatProfilingIter prIter = new ConcatProfilingIter(
                RESOURCE_PATH + "concat-profile-missing-sampleId.txt", neoDao, true);
        int rc = 0;
        String[] row;
        while ((row = prIter.readRow()) != null) {
            rc++;
        }
        System.out.println(rc);
    }

  //  @Test(expected = Base.HeaderException.class)
    public void canDetectMissingVersion() throws Base.ParseException, IOException {
        ConcatProfilingIter prIter = new ConcatProfilingIter(
                RESOURCE_PATH + "concat-profile-missing-version.txt", neoDao, true);
        int rc = 0;
        String[] row;
        while ((row = prIter.readRow()) != null) {
            rc++;
        }
        System.out.println(rc);
    }

   // @Test(expected = Base.HeaderException.class)
    public void canDetectWrongVersion() throws Base.ParseException, IOException {
        ConcatProfilingIter prIter = new ConcatProfilingIter(
                RESOURCE_PATH + "concat-profile-missing-version.txt", neoDao, true);
        int rc = 0;
        String[] row;
        while ((row = prIter.readRow()) != null) {
            rc++;
        }
        System.out.println(rc);
    }

  //  @Test(expected = Base.FieldException.class)
    public void canDetectInvalidProfilingTAXPATH() throws Base.ParseException, IOException {

        Profile.ValidatingReader pr = new Profile.ValidatingReader(
                RESOURCE_PATH + "profile-without-header-invalid-taxpath.txt", neoDao, true);
        int rc = 0;
        String[] row;
        while ((row = pr.readRow()) != null) {
            rc++;
        }
    }

    //@Test(expected = Base.FieldException.class)
    public void canDetectInvalidProfilingRowDef() throws Base.ParseException, IOException {
        Profile.ValidatingReader pr = new Profile.ValidatingReader(
                RESOURCE_PATH + "concat-profile-invalid-row-def.txt", neoDao, true);
        int rc = 0;
        String[] row;
        while ((row = pr.readRow()) != null) {
            rc++;
        }
    }

    @Test
    public void testConcatProfileWithoutTaxpathsn() throws Base.ParseException, IOException {

        ConcatProfilingIter prIter = new ConcatProfilingIter(
                RESOURCE_PATH + "concat-profile-valid-without-taxpathsn.txt", neoDao, true);
        int rc = 0;
        String[] row;
        while ((row = prIter.readRow()) != null) {
            rc++;
        }
        System.out.println(rc);
        assertEquals(24, rc);
    }


    @Test
    public void testConcatProfileWithTaxonomyId() throws Base.ParseException, IOException {

        ConcatProfilingIter prIter = new ConcatProfilingIter(
                RESOURCE_PATH + "concat-profile-valid-taxonomyId.txt", neoDao, true);
        int rc = 0;
        String[] row;
        while ((row = prIter.readRow()) != null) {
            rc++;
        }
        System.out.println(rc);
        assertEquals(24, rc);
    }

  //  @Test(expected = Base.FieldException.class)
    public void testConcatProfileWithWrongRank() throws Base.ParseException, IOException {

        ConcatProfilingIter prIter = new ConcatProfilingIter(
                RESOURCE_PATH + "concat-profile-invalid-wrong-rank.txt", neoDao, true);
        int rc = 0;
        String[] row;
        while ((row = prIter.readRow()) != null) {
            rc++;
        }
        assertEquals(24, rc);
    }

 //   @Test(expected = Base.HeaderException.class)
    public void testConcatProfileWithWrongRanks() throws Base.ParseException, IOException {
        ConcatProfilingIter prIter = new ConcatProfilingIter(
                RESOURCE_PATH + "concat-profile-invalid-wrong-ranks.txt", neoDao, true);
        int rc = 0;
        String[] row;
        while ((row = prIter.readRow()) != null) {
            rc++;
        }
        assertEquals(24, rc);
    }

 //   @Test(expected = Base.FieldException.class)
    public void testConcatProfileNoValueInTaxpath() throws Base.ParseException, IOException {
        ConcatProfilingIter prIter = new ConcatProfilingIter(
                RESOURCE_PATH + "concat-profile-no-value-in-taxpath.txt", neoDao, true);
        int rc = 0;
        String[] row;
        while ((row = prIter.readRow()) != null) {
            rc++;
        }
        assertEquals(24, rc);
    }

 //   @Test(expected = Base.HeaderException.class)
    public void testConcatProfileInvalidWrongColumnOrder() throws Base.ParseException, IOException {
        ConcatProfilingIter prIter = new ConcatProfilingIter(
                RESOURCE_PATH + "concat-profile-invalid-wrong-order.txt", neoDao, true);
        int rc = 0;
        String[] row;
        while ((row = prIter.readRow()) != null) {
            rc++;
        }
        assertEquals(24, rc);
    }

//    @Test(expected = Base.FieldException.class)
//    @Test
    public void testConcatProfiles() throws Base.ParseException, IOException {
        ConcatProfilingIter prIter = new ConcatProfilingIter(
                RESOURCE_PATH + "profiling-concat-test", neoDao, true);
        int rc = 0;
        String[] row;
        while ((row = prIter.readRow()) != null) {
            rc++;
        }
        assertEquals(5, rc);
    }

    @Test
    public void testInvalidPercentage() throws Base.ParseException, IOException {
        ConcatProfilingIter prIter = new ConcatProfilingIter(
                RESOURCE_PATH + "concat-profile-invalid-percentage-number.txt", neoDao, true);
        int rc = 0;
        String[] row;
        while ((row = prIter.readRow()) != null) {
            rc++;
        }
        assertEquals(24, rc);
    }

  //  @Test
    public void testEmptyColumns() throws Base.ParseException, IOException {
        ConcatProfilingIter prIter = new ConcatProfilingIter(
                RESOURCE_PATH + "profiling-concat-test", neoDao, true);
        int rc = 0;
        String[] row;
        while ((row = prIter.readRow()) != null) {
            rc++;
        }
        assertEquals(5, rc);
    }

    @Test
    public void concatProfilingWithPointColumns() throws Base.ParseException, IOException {
        ConcatProfilingIter prIter = new ConcatProfilingIter(
                RESOURCE_PATH + "concat-profile-valid-taxid-with-point.txt", neoDao, true);
        int rc = 0;
        String[] row;
        while ((row = prIter.readRow()) != null) {
            rc++;
        }
        assertEquals(24, rc);
    }

}
