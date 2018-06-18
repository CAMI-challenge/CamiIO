package cami.io;

import mzd.taxonomy.neo.NeoDao;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Concrete implementation of the Taxonomic Binning format.
 */
public class Binning extends Base {
    private final static String BIN_TASK = "binning";
    private final static String BIN_VERSION_SUPPORT[] = {"0.9"};
    private final static String REFBASED_KEY = "referencebased";
    private final static String ASMBASED_KEY = "assemblybased";
    private final static String REPINFO_KEY = "replicateinfo";
    private final static String SAMPLEID_KEY = "sampleid";
    private final static String[] BIN_MANDATORY_FIELDS = {VERSION_KEY, SAMPLEID_KEY};

    /**
     * A few common predefined column definition formats.
     */
    public final static String BINID = "BINID";
    public final static String TAXID = "TAXID";
    public final static String SEQUENCEID = "SEQUENCEID";
    public final static String[] COLUMNDEF_TAXID = {SEQUENCEID, TAXID};
    public final static String[] COLUMNDEF_BINID = {SEQUENCEID, BINID};
    public final static String[] COLUMNDEF_TAXID_AND_BINID = {SEQUENCEID, TAXID, BINID};

    /**
     * Writer for CAMI Binning format.
     */
    public static class Writer extends Base.Writer {
        /**
         * Constructor writing binning information to file.
         *
         * @param fileName         - the file name for writing.
         * @param columnDefinition - the explicit column definition.
         * @param create           - create a new file (otherwise append).
         * @throws IOException
         */
        public Writer(String fileName, String[] columnDefinition, boolean create) throws IOException {
            super(fileName, columnDefinition, create);
            setTask(BIN_TASK);
            setVersion("1.0");
            setReferenceBased(false);
            setAssemblyBased(false);
            setReplicateInfo(false);
        }

        /**
         * This convenience constructor has been deprecated as explicit mention of the column format is
         * probably necessary. The column format for this constructor is:
         * <p/>
         * {@code SEQUENCEID TAXID BINID}
         * <p/>
         *
         * @param fileName - the file to open for writing
         * @throws ParseException
         * @throws IOException
         */
        @Deprecated
        public Writer(String fileName) throws ParseException, IOException {
            this(fileName, new String[]{}, false);
        }

        public void setReferenceBased(boolean val) {
            getHeaderInfo().put(Binning.REFBASED_KEY, val ? "T" : "F");
        }

        public boolean isReferenceBased() {
            return "T".equals(getHeaderInfo().get(REFBASED_KEY));
        }

        public void setAssemblyBased(boolean val) {
            getHeaderInfo().put(Binning.ASMBASED_KEY, val ? "T" : "F");
        }

        public boolean isAssemblyBased() {
            return "T".equals(getHeaderInfo().get(ASMBASED_KEY));
        }

        public void setReplicateInfo(boolean val) {
            getHeaderInfo().put(Binning.REPINFO_KEY, val ? "T" : "F");
        }

        public boolean isReplicateInfo() {
            return "T".equals(getHeaderInfo().get(REPINFO_KEY));
        }
    }

    /**
     * Reader for CAMI Binning format
     */
    public static class Reader extends Base.Reader {
        public Reader(String fileName, Boolean checkHeader) throws ParseException, IOException {
            super(fileName, BIN_TASK, BIN_VERSION_SUPPORT, BIN_MANDATORY_FIELDS, checkHeader);
        }
    }

    public static class ValidatingReader extends Base.Reader {
        private NeoDao neoDao;
        // a local neoDao instance will be shutdown with close()
        private boolean localNeoDao = false;
        private static int taxIDIndex = -1;

        public ValidatingReader(String fileName, NeoDao neoDao, Boolean checkHeader)
                throws ParseException, IOException {
            super(fileName, BIN_TASK, BIN_VERSION_SUPPORT, BIN_MANDATORY_FIELDS, checkHeader);
            this.neoDao = neoDao;
        }

        public ValidatingReader(String fileName, String neoDBPath, Boolean checkHeader)
                throws ParseException, IOException {
            super(fileName, BIN_TASK, BIN_VERSION_SUPPORT, BIN_MANDATORY_FIELDS, checkHeader);
            this.neoDao = new NeoDao(new File(neoDBPath));
            this.localNeoDao = true;
        }

        /**
         * Read the entire header record.
         *
         * @throws ParseException header was invalid in some manner
         * @throws IOException    error reading from file
         */
        @Override
        protected void readHeader() throws ParseException, IOException {
            super.readHeader();
            //set index of BINID columns (if provided)
            taxIDIndex = columnDefinition.indexOf("TAXID");
            equalsPattern();
        }

        private void equalsPattern() throws ParseException {
            // check for the following allowed IDs
            // "SEQUENCEID", "TAXID"
            // "SEQUENCEID", "BINID"
            List<String> firstTwoCol = columnDefinition.subList(0, 2);
            if (!firstTwoCol.equals(Arrays.asList(COLUMNDEF_BINID)) &&
                    !firstTwoCol.equals(Arrays.asList(COLUMNDEF_TAXID))) {
                getLogger().warn("Invalid Header on line: {}  \n", lineNumber);
                throw new HeaderException("");
            }

            for (String col : columnDefinition) {
                if (!col.equals(BINID) && !col.equals(TAXID) && !col.equals(SEQUENCEID)
                        && !col.matches("_[A-Za-z]*_[A-Za-z]+[A-Za-z0-9]*")) {
                    getLogger().warn("Invalid Header on line:{} . " +
                            "Custom types MUST be prefixed by a case-insensitive " +
                            "string with an underscore before and after the string  ", lineNumber);
                    throw new HeaderException("");
                }
            }
        }

        @Override
        public String[] readRow() throws ParseException, IOException {
            String[] values = super.readRow();
            if (values != null) {
                if (taxIDIndex != -1) {
                    try {
                        int taxId = toInt(values[taxIDIndex]);
                        if (!getNeoDao().taxonExists(taxId)) {
                            getLogger().warn("Invalid TAXID [{}] on line:{}", taxId, lineNumber);
                            throw new FieldException("");
                        }
                    } catch (NumberFormatException ex) {
                        getLogger().warn("Invalid TAXID on line:{}", lineNumber);
                        throw new FieldException("");
                    }
                }
            }
            return values;
        }

        /**
         * Close the underlying FileReader and shutdown any local
         * NeoDao instance. This will not close an external NeoDao
         * instance passed in at instantiation time.
         */
        @Override
        public void close() throws IOException {
            super.close();
            if (isLocalNeoDao() && getNeoDao() != null) {
                getNeoDao().shutdown();
            }
        }

        public boolean isLocalNeoDao() {
            return localNeoDao;
        }

        public NeoDao getNeoDao() {
            return neoDao;
        }
    }
}
