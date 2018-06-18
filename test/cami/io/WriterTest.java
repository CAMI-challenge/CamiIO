package cami.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import cami.io.Base.FieldException;
import cami.io.Base.HeaderException;
import cami.io.Base.ParseException;

import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class WriterTest extends TestResources {

	private List<String[]> bin_rows;
	private List<String[]> pro_rows;
	
	@Before
	public void setUp() throws ParseException, IOException {
		String[] row;
		
		bin_rows = new ArrayList<String[]>();
		Binning.Reader br = new Binning.Reader(RESOURCE_PATH +"binning-valid.txt",true);
		while ((row = br.readRow()) != null) {
			bin_rows.add(row);
		}
		br.close();
		
		pro_rows = new ArrayList<String[]>();
		Profile.Reader pr = new Profile.Reader(RESOURCE_PATH +"profile-valid.txt",true);
		while ((row = pr.readRow()) != null) {
			pro_rows.add(row);
		}
		pr.close();
	}
	
	//@Test
	public void testAA_WriteValid() throws ParseException, IOException {
		Binning.Writer bw = new Binning.Writer(RESOURCE_PATH +"binning-delete.txt",Binning.COLUMNDEF_TAXID,true);
		bw.writeHeader();
		for (String[] r : bin_rows) {
			bw.writeRow(r);
		}
		bw.close();

		Profile.Writer pw = new Profile.Writer(RESOURCE_PATH +"profile-delete.txt", true);
		pw.writeHeader();
		for (String[] r : pro_rows) {
			pw.writeRow(r);
		}
		pw.close();
	}

//	@Test(expected=IOException.class)
//	public void testBinNoCreate() throws IOException {
//		
//		new Binning.Writer("src/test/resources/binning-delete.txt", false);
//	}

	//@Test
	public void testGetHeader() throws IOException {
	    Binning.Writer  writer = new Binning.Writer("", new String[]{},false);
		writer.setAssemblyBased(true);
		writer.setContestantId("id");
		writer.setReferenceBased(true);
		writer.setReplicateInfo(true);
		writer.setSampleId("id");
		writer.setTask("task");
		writer.setVersion("version");
	    String header = writer.getHeader();
	    assertEquals(header,"#CAMI Format for Binning\n" +
				"@sampleid:id\n" +
				"@referencebased:T\n" +
				"@task:task\n" +
				"@replicateinfo:T\n" +
				"@contestantid:id\n" +
				"@assemblybased:T\n" +
				"@version:version\n");
	}
	
//	@Test(expected=IOException.class)
//	public void testProNoOverwrite() throws IOException {
//		new Profile.Writer("src/test/resources/profile-delete.txt", false);
//	}

//	@Test(expected=Base.HeaderException.class)
	public void testBinUnkHeader() throws IOException, HeaderException {
		Binning.Writer br = new Binning.Writer(RESOURCE_PATH +"binning-delete.txt", new String[]{}, true);
		br.setHeaderInfo("foo","bar");
	}

//	@Test(expected=Base.HeaderException.class)
	public void testProUnkHeader() throws IOException, HeaderException {
		Profile.Writer pr = new Profile.Writer(RESOURCE_PATH + "profile-delete.txt", true);
		pr.setHeaderInfo("foo","bar");
	}
	
//	@Test(expected=Base.FieldException.class)
	public void testBinBadField() throws IOException, FieldException, HeaderException {
		Binning.Writer br = new Binning.Writer(RESOURCE_PATH + "binning-delete.txt", new String[]{}, true);
		br.writeHeader();
		br.writeRow(new String[]{"1","2","3","4","5","6","7","8","9","0"});
	}
	
//	@Test(expected=Base.FieldException.class)
	public void testProBadField() throws IOException, FieldException, HeaderException {
		Profile.Writer pr = new Profile.Writer(RESOURCE_PATH + "profile-delete.txt", true);
		pr.writeHeader();
		pr.writeRow(new String[]{"1","2","3","4","5","6","7","8","9","0"});
	}
}
