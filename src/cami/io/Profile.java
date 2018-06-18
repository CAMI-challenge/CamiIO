package cami.io;

import mzd.taxonomy.neo.NeoDao;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Concrete implementation of the Taxonomic Profile format.
 */
public class Profile extends Base {

    // Profile constants
    private final static String PRO_TASK = "profiling";
    private final static String PRO_VERSION_SUPPORT[] = {"0.9"};
    public final static String RANKS_KEY = "ranks";
    private final static String RANKS_COL_DEF = "RANK";
    private final static String TAXID_COL_DEF = "TAXID";
    private final static String TAXPATH_COL_DEF = "TAXPATH";
    public final static String TAXPATH_SN_COL_DEF = "TAXPATHSN";
    private final static String PERCENTAGE_COL_DEF = "PERCENTAGE";
    private final static String PRO_COLUMN_DEFINITION[] = {TAXID_COL_DEF, RANKS_COL_DEF, TAXPATH_COL_DEF,
            PERCENTAGE_COL_DEF};
    private final static String PRO_COLUMN_DEFINITION_TAXPATH_SN[] = {TAXID_COL_DEF, RANKS_COL_DEF, TAXPATH_COL_DEF,
            TAXPATH_SN_COL_DEF, PERCENTAGE_COL_DEF};
    private final static String PRO_MANDATORY_FIELDS[] = {SAMPLEID_KEY, VERSION_KEY, RANKS_KEY,};
    public final static String PRO_RANKS = "superkingdom|phylum|class|order|family|genus|species|strain";

    /**
     * Writer for CAMI Profile format.
     */
    public static class Writer extends Base.Writer {

        public Writer(String fileName, boolean create) throws IOException {
            super(fileName, PRO_COLUMN_DEFINITION, create);
            setTask(PRO_TASK);
            setVersion("1.0");
            setRanks(PRO_RANKS);
        }

        public Writer(String fileName) throws ParseException, IOException {
            super(fileName, PRO_COLUMN_DEFINITION, false);
        }

        public String getRanks() {
            return getHeaderInfo().get(RANKS_KEY);
        }

        public void setRanks(String ranks) {
            getHeaderInfo().put(RANKS_KEY, ranks);
        }
    }

    /**
     * Profile Reader without any validation of underlying taxonomic ids or lineages.
     */
    public static class Reader extends Base.Reader {

        public Reader(String fileName, Boolean checkHeader) throws ParseException, IOException {
            super(fileName, PRO_TASK, PRO_VERSION_SUPPORT,
                    PRO_MANDATORY_FIELDS, checkHeader);
        }
    }

    /**
     * Profile Reader which also validates taxonomic IDs and lineages against
     * the NCBI taxonomy.
     * <p/>
     * Instances of this class depend on {@code mzd.taxonomy.neo.NeoDao}. This can either
     * be created at ValidatingReader instantiation time or an external object can be
     * supplied.
     * <p/>
     * As instantiation overhead exists for the reusable NeoDao object, it is recommended
     * that managing objects supply it as a <b>singleton</b> instance -- as embedded Neo4j databases
     * permit only one accessor at any one time.
     */
    public static class ValidatingReader extends Base.Reader {
        private NeoDao neoDao;
        // a local neoDao instance will be shutdown with close()
        private boolean localNeoDao = false;


        public ValidatingReader(String fileName, NeoDao neoDao,
                                Boolean checkHeader) throws ParseException, IOException {

            super(fileName, PRO_TASK, PRO_VERSION_SUPPORT,
                    PRO_MANDATORY_FIELDS, checkHeader);
            this.neoDao = neoDao;
        }

        public ValidatingReader(String fileName, String neoDBPath,
                                Boolean checkHeader) throws ParseException, IOException {

            super(fileName, PRO_TASK, PRO_VERSION_SUPPORT,
                    PRO_MANDATORY_FIELDS, checkHeader);
            this.neoDao = new NeoDao(new File(neoDBPath));
            this.localNeoDao = true;
        }

        @Override
        public String[] readRow() throws ParseException, IOException {

            String[] values = super.readRow();
            if (values == null) {
                // end of file reached
                return null;
            }

            // First column in row is a TAXID, check that it exists.
            // This is technically redundant if the lineage field includes
            // this value as well and is validated.
            int taxId = toInt(values[0]);
            this.checkInvalidTAXID(taxId);
            this.checkInvalidTaxPath(values[2]);
            return values;
        }

        @Override
        protected void readHeader() throws ParseException, IOException {
            super.readHeader();
            //set index of BINID columns (if provided)
            equalsPattern();
        }

        private void equalsPattern() throws ParseException {
//          check for the following allowed IDs
//          TAXID, RANKS, TAXPATH,PERCENTAGE
//          TAXID, RANKS, TAXPATH, TAXPATHSN, PERCENTAGE
            List<String> subCol = null;
            List<String> newColDef = new ArrayList<>();
            for (String col : columnDefinition) {
                newColDef.add(col.toUpperCase());
            }

            //this MUST be refactored
            columnDefinition = newColDef;
            if (columnDefinition.subList(0, PRO_COLUMN_DEFINITION.length).equals(Arrays.asList(PRO_COLUMN_DEFINITION))) {
                subCol = columnDefinition.subList(0, PRO_COLUMN_DEFINITION.length);
                percentageColumn = 3;
            }

            if (columnDefinition.size() >= PRO_COLUMN_DEFINITION_TAXPATH_SN.length &&
                    columnDefinition.subList(0, PRO_COLUMN_DEFINITION_TAXPATH_SN.length).equals(Arrays.asList(PRO_COLUMN_DEFINITION_TAXPATH_SN))) {
                subCol = columnDefinition.subList(0, PRO_COLUMN_DEFINITION_TAXPATH_SN.length);
                percentageColumn = 4;
            }

            if (subCol == null) {
                getLogger().warn("Invalid Header on line: {}  \n", lineNumber);
                throw new HeaderException("");
            }

            if (subCol.equals(PRO_COLUMN_DEFINITION) && subCol.size() > PRO_COLUMN_DEFINITION.length) {
                List<String> subColumns = columnDefinition.subList(PRO_COLUMN_DEFINITION.length, columnDefinition.size());
                checkCustomColumns(subColumns, lineNumber);
            }

            if (subCol.equals(PRO_COLUMN_DEFINITION_TAXPATH_SN) && columnDefinition.size() > PRO_COLUMN_DEFINITION_TAXPATH_SN.length) {
                List<String> subColumns = columnDefinition.subList(PRO_COLUMN_DEFINITION_TAXPATH_SN.length, columnDefinition.size());
                checkCustomColumns(subColumns, lineNumber);
            }
        }

        private void checkCustomColumns(List<String> customColumns, int lineNumber) throws HeaderException {
            for (String column : customColumns) {
                if (!column.matches("n _[A-Za-z]*_[A-Za-z]+[A-Za-z0-9]*")) {
                    getLogger().warn("Invalid Header on line:{} . " +
                            "Custom types MUST be prefixed by a case-insensitive " +
                            "string with an underscore before and after the string  ", lineNumber);
                    throw new HeaderException("");
                }
            }
        }

        protected void checkInvalidTAXID(int taxId) throws FieldException {
            if (!getNeoDao().taxonExists(taxId)) {
                getLogger().warn("Invalid TAXID [{}] on line:{}", taxId, lineNumber);
                throw new FieldException("");
            }
        }

        protected void checkInvalidTaxPath(String taxpath) throws FieldException {
            // Check that the lineage field exists as well.
            if (taxpath.trim().isEmpty()) {
                getLogger().warn("Invalid TAXPATH {} on line:{}", taxpath, lineNumber);
                throw new FieldException("");
            }
            List<Integer> lineage = toIntList(taxpath, "\\|");
            Integer first = null;
            Integer second = null;

            for (Integer taxId : lineage) {
                second = taxId;
                if (getNeoDao().taxonExists(second)) {
                    if (first != null && second != null &&
                            (!getNeoDao().sparsePathExists(first, second))) {
                        getLogger().warn("Invalid TAXPATH {} on line:{}", taxpath, lineNumber);
                        throw new FieldException("");
                    }
                    first = second;
                } else {
                    getLogger().warn("Invalid TAXID {} on line:{}", taxpath, lineNumber);
                    throw new FieldException("");
                }
            }
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
