package cami.io;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import mzd.taxonomy.neo.NeoDao;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import cami.io.Base.ParseException;

public class ReaderTest extends TestResources {

	private static NeoDao neoDao = null;
	
	@BeforeClass
	public static void openDB() throws ParseException, IOException, InterruptedException {
		neoDao = new NeoDao(new File(DBPATH));
	}
	
	@AfterClass
	public static void closeDB() throws IOException {
		if (neoDao != null) {
			neoDao.shutdown();
		}
	}
	
	
	@Test(expected = Base.ParseException.class)
    public void testEmpty() throws IOException, ParseException {
		Binning.Reader br = new Binning.Reader(
				RESOURCE_PATH + "binning-empty.txt", true);
		while (br.readRow() != null)
			;

		Profile.ValidatingReader pr = new Profile.ValidatingReader(
				RESOURCE_PATH + "profile-empty.txt", neoDao, true);
		while (pr.readRow() != null)
			;
	}

	@Test(expected = Base.HeaderException.class)
    public void testDupKey() throws IOException, ParseException {
		Binning.Reader br = new Binning.Reader(
				RESOURCE_PATH + "dup-key.txt", true);
		while (br.readRow() != null)
			;
	}

	@Test(expected = Base.HeaderException.class)
    public void testMissKey() throws IOException, ParseException {
		Binning.Reader br = new Binning.Reader(
				RESOURCE_PATH + "missing-key.txt", true);
		while (br.readRow() != null)
			;
	}

	@Test(expected = Base.HeaderException.class)
    public void testMissVal() throws IOException, ParseException {
		Binning.Reader br = new Binning.Reader(
				RESOURCE_PATH + "missing-val.txt", true);
		while (br.readRow() != null)
			;
	}

	@Test(expected = Base.HeaderException.class)
    public void testNoColumnBinning() throws IOException, ParseException {
		Binning.ValidatingReader br = new Binning.ValidatingReader(
				RESOURCE_PATH + "binning-no-coldef.txt", neoDao, true);
		while (br.readRow() != null)
			;
	}

    @Test(expected = Base.HeaderException.class)
    public void testNoColumnProfile() throws IOException, ParseException {
        Profile.ValidatingReader pr = new Profile.ValidatingReader(
                RESOURCE_PATH + "profile-no-coldef.txt", neoDao, true);
        while (pr.readRow() != null)
            ;
    }


    @Test(expected = Base.HeaderException.class)
    public void testUnkVerBinning() throws IOException, ParseException {
		Binning.ValidatingReader br = new Binning.ValidatingReader(
				RESOURCE_PATH + "binning-unk-ver.txt", neoDao,true);
		while (br.readRow() != null)
			;
	}

    @Test(expected = Base.HeaderException.class)
    public void testUnkVerProfiling() throws IOException, ParseException {
		Profile.ValidatingReader pr = new Profile.ValidatingReader(
				RESOURCE_PATH + "profile-unk-ver.txt", neoDao, true);
		while (pr.readRow() != null)
			;
    }

	@Test(expected = Base.FieldException.class)
	public void testBadRowBinnning() throws IOException, ParseException {
		Binning.ValidatingReader br = new Binning.ValidatingReader(
				RESOURCE_PATH + "binning-bad-row.txt", neoDao ,true);
		while (br.readRow() != null)
			;
	}

    @Test(expected = Base.FieldException.class)
    public void testBadRowProfiling() throws IOException, ParseException {
		Profile.ValidatingReader pr = new Profile.ValidatingReader(
				RESOURCE_PATH + "profile-bad-row.txt", neoDao, true);
		while (pr.readRow() != null)
			;
    }

	@Test
	public void testBinningValid() throws ParseException, IOException {

		Binning.ValidatingReader reader = new Binning.ValidatingReader(
				RESOURCE_PATH + "binning-valid.txt",neoDao ,true);
		int rc = 0;
		String[] row;
		while ((row = reader.readRow()) != null) {
			assertEquals(3, row.length);
			rc++;
		}
		reader.close();
		assertEquals(5, rc);
	}

    @Test
	public void testBinningWithoutHeaderValid() throws ParseException, IOException {

		Binning.ValidatingReader reader = new Binning.ValidatingReader(
				RESOURCE_PATH + "binning-without-header-valid.txt", neoDao, false);
		int rc = 0;
		String[] row;
		while ((row = reader.readRow()) != null) {
			assertEquals(3, row.length);
			rc++;
		}
		reader.close();
		assertEquals(5, rc);
	}
	
	@Test(expected = Base.HeaderException.class)
	public void testBinningWithoutHeaderNoColumn() throws IOException, ParseException {
		Binning.ValidatingReader reader = new Binning.ValidatingReader(
				RESOURCE_PATH + "binning-without-header-no-col-def.txt", neoDao, true);
		int rc = 0;
		String[] row;
		while ((row = reader.readRow()) != null) {
			assertEquals(3, row.length);
			rc++;
		}
		reader.close();
		assertEquals(5, rc);
	}

	@Test(expected = Base.HeaderException.class)
    public void testBinningWrongColumnDef() throws IOException, ParseException {
		Binning.ValidatingReader reader = new Binning.ValidatingReader(
				RESOURCE_PATH + "binning-without-header-wrong-col-def.txt",neoDao, false);
		int rc = 0;
		String[] row;
		while ((row = reader.readRow()) != null) {
			//assertEquals(3, row.length);
			rc++;
		}
		reader.close();
		assertEquals(5, rc);
	}

	@Test
	public void testProfileValid() throws ParseException, IOException {

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

//	@Test
	public void testProfileWithoutHeaderValid() throws ParseException, IOException {

		Profile.ValidatingReader pr = new Profile.ValidatingReader(
				RESOURCE_PATH + "profile-without-header-valid.txt", neoDao, false);
		int rc = 0;
		String[] row;
		while ((row = pr.readRow()) != null) {
			assertEquals(5, row.length);
			rc++;
		}
		assertEquals(12, rc);
	}

	@Test(expected = Base.FieldException.class)
    public void canDetectInvalidProfilingID() throws ParseException, IOException {

		Profile.ValidatingReader pr = new Profile.ValidatingReader(
				RESOURCE_PATH + "profile-without-header-invalid-id.txt", neoDao, false);
		int rc = 0;
		String[] row;
		while ((row = pr.readRow()) != null) {
			rc++;
		}
	}


	@Test(expected = Base.FieldException.class)
    public void canDetectInvalidBinningID() throws ParseException, IOException {

		Binning.ValidatingReader br = new Binning.ValidatingReader(
				RESOURCE_PATH + "binning-without-header-invalid-taxId.txt", neoDao ,false);
		int rc = 0;
		String[] row;
		while ((row = br.readRow()) != null) {
			rc++;
		}
	}

	@Test(expected = Base.FieldException.class)
	public void canDetectInvalidProfilingTAXPATH() throws ParseException, IOException {

		Profile.ValidatingReader pr = new Profile.ValidatingReader(
				RESOURCE_PATH + "profile-without-header-invalid-taxpath.txt", neoDao, false);
		int rc = 0;
		String[] row;
		while ((row = pr.readRow()) != null) {
			rc++;
		}
	}

    @Test
    public void canPassBinningTAXId() throws ParseException, IOException {
        Binning.ValidatingReader pr = new Binning.ValidatingReader(
                RESOURCE_PATH + "binning-valid-new-with-point.txt", neoDao, false);
        int rc = 0;
        String[] row;
        while ((row = pr.readRow()) != null) {
            rc++;
        }
    }
}
