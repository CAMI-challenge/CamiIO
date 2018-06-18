package cami.io;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CAMI Challenge IO
 * <p/>
 * This class serves as the basis for reading and writing CAMI Challenge file
 * formats. These files are tab-delimited data with a preceding header.
 * <p/>
 * Information in the header can be declared mandatory and is checked on read
 * and write.
 * <p/>
 * Concrete implementations can be found at: {@link Binning} and {@link Profile}
 *
 * @author Matthew DeMaere
 */
public abstract class Base {
    // reserved symbols
    protected final static String HEADER_COMMENT = "#CAMI Format for Binning";
    protected final static String COMMENT_CHAR = "#";
    protected final static String HEADER_CHAR = "@";
    protected final static String COLUMN_DEF = "@@";
    public final static String DELIMITER = "\t";
    protected final static String HEADER_SEP = ":";
    protected final static String NEWLINE = "\n";
    public static int percentageColumn = -1;

    // generic header keys
    protected final static String TASK_KEY = "task";
    protected final static String VERSION_KEY = "version";
    protected final static String CONID_KEY = "contestantid";
    protected final static String SAMPLEID_KEY = "sampleid";
    protected final static String TAXONOMYID_KEY = "taxonomyid";
    protected final static String RANKS_KEY = "ranks";

    /**
     * Test if a line is blank
     *
     * @param str
     * @return
     */
    public static boolean isBlank(String str) {
        return StringUtils.isBlank(str);
    }

    /**
     * Test if a line is a comment line. (begins with: {@link #COMMENT_CHAR})
     *
     * @param str
     * @return
     */
    public static boolean isComment(String str) {
        return str.startsWith(COMMENT_CHAR);
    }

    /**
     * Test if a line is a header line (begins with: {@link #HEADER_CHAR})
     *
     * @param str
     * @return
     */
    public static boolean isHeaderLine(String str) {
        return str.startsWith(HEADER_CHAR);
    }

    /**
     * Test if the line is the column definitions line. (begins with
     * {@link #COLUMN_DEF})
     *
     * @param str
     * @return
     */
    protected static boolean isColumnsDef(String str) {
        return str.startsWith(COLUMN_DEF);
    }

    /**
     * Join an array of strings with {@link #DELIMITER}
     *
     * @param values - strings to join
     * @return
     * @deprecated Use String.join(DELIMITER, values) directly!
     */
    protected static String join(String[] values) {
        return String.join(DELIMITER, values);
    }

    /**
     * Abstract class for writing CAMI style tabular data.
     * <p/>
     * Concrete classes need only call the super class and add additional
     * field/header definitions.
     * <p/>
     * The header is written manually with {@link #writeHeader()} Data rows are
     * written iteratively with {@link #writeRow(String[])} The file is not
     * automatically closed.
     */
    public static abstract class Writer {
        private BufferedWriter writer = null;
        private Map<String, String> headerInfo = new HashMap<>();
        private String[] columnDefinition;
        private boolean headerWritten;
        private Logger logger = LoggerFactory.getLogger(getClass());

        /**
         * Create a write for output. The underlying
         * {@link java.io.BufferedWriter} is opened at instantiation.
         *
         * @param fileName         - the file to open
         * @param columnDefinition - defines the column definition for this file type
         * @param create           - create the file.
         * @throws IOException
         */
        protected Writer(String fileName, String[] columnDefinition, boolean create) throws IOException {
            this.headerInfo.put(TASK_KEY, "");
            this.headerInfo.put(VERSION_KEY, "");
            this.headerInfo.put(CONID_KEY, "");
            this.headerInfo.put(SAMPLEID_KEY, "");
            this.columnDefinition = columnDefinition;

            // File file = new File(fileName);
            if (create) {
                this.writer = new BufferedWriter(new FileWriter(fileName, false));
            }
            this.headerWritten = false;
        }

        /**
         * Write a line to the file, adding a newline.
         *
         * @param line - the string to write as a line
         * @throws IOException
         */
        private void writeLine(String line) throws IOException {
            this.writer.write(line + "\n");
        }

        /**
         * Write a complete header record to the file. This must be called
         * before can rows are written and can only be called once.
         *
         * @throws IOException     error writing to file
         * @throws HeaderException multiple calls
         */
        public void writeHeader() throws IOException, HeaderException {
            if (this.headerWritten) {
                throw new HeaderException(
                        "header has already been written once");
            }
            writeLine(HEADER_COMMENT);
            for (String k : this.headerInfo.keySet()) {
                writeLine(HEADER_CHAR + k + HEADER_SEP + this.headerInfo.get(k));
            }
            writeLine(COLUMN_DEF + join(this.columnDefinition));
            this.headerWritten = true;
        }

        public String getHeader() {
            String out = HEADER_COMMENT + NEWLINE;
            for (String k : this.headerInfo.keySet()) {
                out += HEADER_CHAR + k + HEADER_SEP + this.headerInfo.get(k)
                        + NEWLINE;
            }
            return out;
        }

        /**
         * Write a data row to the file. The row must agree in number to the
         * number of columns defined.
         *
         * @param row the row of tab-delimited data to write.
         * @throws FieldException  wrong number of fields.
         * @throws HeaderException header has not been written first.
         * @throws IOException     error writing to file.
         */
        public void writeRow(String[] row) throws FieldException,
                HeaderException, IOException {
            if (!this.headerWritten) {
                throw new HeaderException(
                        "header must be written prior to data rows");
            }
            if (row.length != this.columnDefinition.length) {
                throw new FieldException("number of fields in:"
                        + Arrays.toString(row)
                        + " does not agree with columns:"
                        + Arrays.toString(this.columnDefinition));
            }
            writeLine(join(row));
        }

        /**
         * Close the underlying {@link java.io.BufferedWriter}.
         *
         * @throws IOException
         */
        public void close() throws IOException {
            if (this.writer != null) {
                this.writer.close();
            }
        }

        /**
         * Set a header field value. These fields must exist for the defined
         * concrete class, otherwise an exception is thrown.
         *
         * @param key   the header field to set
         * @param value the value for the specified field
         * @throws HeaderException header field has not been defined
         */
        public void setHeaderInfo(String key, String value)
                throws HeaderException {
            if (!getHeaderInfo().containsKey(key)) {
                throw new HeaderException(String.format(
                        "unknown header info field %s", key));
            }
        }

		/*
         * Get/Set methods
		 */

        protected Map<String, String> getHeaderInfo() {
            return headerInfo;
        }

        public String getTask() {
            return getHeaderInfo().get(TASK_KEY);
        }

        public void setTask(String task) {
            getHeaderInfo().put(TASK_KEY, task);
        }

        public String getVersion() {
            return getHeaderInfo().get(VERSION_KEY);
        }

        public void setVersion(String version) {
            getHeaderInfo().put(VERSION_KEY, version);
        }

        public String getContestantId() {
            return getHeaderInfo().get(CONID_KEY);
        }

        public void setContestantId(String contestantId) {
            getHeaderInfo().put(CONID_KEY, contestantId);
        }

        public String getSampleId() {
            return getHeaderInfo().get(SAMPLEID_KEY);
        }

        public void setSampleId(String sampleId) {
            getHeaderInfo().put(SAMPLEID_KEY, sampleId);
        }

        public Logger getLogger() {
            return logger;
        }

    }

    /**
     * Abstract class for reading CAMI style tabular data.
     * <p/>
     * Concrete classes need only call the super class and add additional
     * field/header definitions.
     * <p/>
     * The header is read immediately. Data rows are read iteratively with
     * {@link #readRow()} The file is not automatically closed.
     */
    public static abstract class Reader {
        public boolean isTaxPathSnUsed = false;
        protected int lineNumber = 0;
        protected BufferedReader reader = null;
        private Map<String, String> headerInfo = new HashMap<>();
        protected List<String> columnDefinition = null;
        private List<String> mandatoryFields = new ArrayList<>();
        private Map<String, List<String>> supports = new HashMap<>();
        protected Boolean checkHeader;
        private Logger logger = LoggerFactory.getLogger(getClass());

        /**
         * Create a Reader for CAMI tabular data with <b>**explicit column definitions**</b>.
         *
         * The explicit definition is specified at creation time. A file must conform to
         * this specified definition or an expection will be thrown.
         *
         * Concrete classes can extend this class, adding specific details.
         *
         * @param filename
         *            the file to open
         * @param taskName
         *            the new of the file type definition
         * @param versionSupport
         *            list of supported versions for the given specification
         * @param extraMandatoryFields
         *            mandatory fields beyond the basis
         * @param columnDefinition
         *            data column names
         * @param checkHeader
         *            check header fields
         * @throws ParseException
         *             error while reading the header
         * @throws IOException
         *             error reading from file
         */
//		public Reader(String filename, String taskName, String[] versionSupport, String[] extraMandatoryFields,
//				String[] columnDefinition, Boolean checkHeader) throws ParseException, IOException {
//  		this.supports.put(TASK_KEY, Arrays.asList(taskName));
//			this.supports.put(VERSION_KEY, Arrays.asList(versionSupport));
//          this.columnDefinition = Arrays.asList(columnDefinition);
//			this.mandatoryFields.addAll(Arrays.asList(extraMandatoryFields));
//
//			this.reader = new BufferedReader(new FileReader(filename));
//			this.checkHeader = checkHeader;
//			readHeader();
//		}

        /**
         * Create a Reader for CAMI tabular data with <b>**implicit column definitions**</b>.
         * <p/>
         * The column definition is read from the file header. All subsequent rows must conform
         * to the definition found within the file or an expection will be thrown.
         * <p/>
         * Concrete classes can extend this class, adding specific details.
         *
         * @param filename             the file to open
         * @param taskName             the new of the file type definition
         * @param versionSupport       list of supported versions for the given specification
         * @param extraMandatoryFields mandatory fields beyond the basis
         * @param checkHeader          check header fields
         * @throws ParseException error while reading the header
         * @throws IOException    error reading from file
         */
        public Reader(String filename, String taskName, String[] versionSupport, String[] extraMandatoryFields,
                      Boolean checkHeader) throws ParseException, IOException {
            this.supports.put(TASK_KEY, Arrays.asList(taskName));
            this.supports.put(TAXONOMYID_KEY, Arrays.asList(taskName));
            this.supports.put(VERSION_KEY, Arrays.asList(versionSupport));
            this.supports.put(Profile.RANKS_KEY, Arrays.asList(Profile.PRO_RANKS));
            this.mandatoryFields.addAll(Arrays.asList(extraMandatoryFields));

            this.reader = new BufferedReader(new FileReader(filename));
            this.checkHeader = checkHeader;
            readHeader();
        }

        /**
         * Read a line from the file, trimming whitespace and tracking line
         * number.
         *
         * @return the trimmed line or null at EOF
         * @throws IOException error reading from file
         */
        protected String readLine() throws IOException {
            String line = this.reader.readLine();
            if (line != null) {
                this.lineNumber++;
            }
            return line;
        }

        /**
         * Check that the mandatory set of header fields exist.
         *
         * @throws HeaderException missing field
         */
        protected void checkMandatorySet() throws HeaderException {
            for (String mf : this.mandatoryFields) {
                if (!this.headerInfo.containsKey(mf.toLowerCase())) {
                    throw new HeaderException(String.format("mandatory header field %s was not found", mf));
                }
            }
        }

        /**
         * Parse a header line for validity and store the result in
         * {@link #headerInfo}
         *
         * @param line the line to parse
         * @throws HeaderException error while parsing the line
         */
        protected void parseHeaderLine(String line) throws HeaderException {
            if (line.length() < 4 || StringUtils.countMatches(line, HEADER_SEP) != 1) {
                throw new HeaderException(String.format("malformed header line [%s]", line));
            }

            String[] tok = line.substring(1).split(HEADER_SEP);
            if (tok.length != 2) {
                throw new HeaderException(
                        String.format(
                                "header does not appear to contain a key/value pair. line:%d [%s]",
                                this.lineNumber, line));
            }
            if (StringUtils.isEmpty(tok[0])) {
                throw new HeaderException(
                        String.format(
                                "header does not appear to contain a key. line:%d [%s]",
                                this.lineNumber, line));
            } else if (StringUtils.isEmpty(tok[1])) {
                throw new HeaderException(
                        String.format(
                                "header does not appear to contain a value. line:%d [%s]",
                                this.lineNumber, line));
            }
            String key = tok[0].toLowerCase();
            String value = tok[1].toLowerCase();

            if (this.headerInfo.containsKey(key)) {
                throw new HeaderException(String.format(
                        "duplicate header key used. line:%d [%s]",
                        this.lineNumber, line));
            }

            // store valid entry
            this.headerInfo.put(key.toLowerCase(), value);

            // check against the those fields which have declared support
            for (Map.Entry<String, List<String>> entry : this.supports.entrySet()) {
//                logger.info("Entrykey: " + entry.getKey());
//                logger.info("normal key: " + key);
                if (key.equals(VERSION_KEY)) {
                    List<String> versionValues = this.supports.get(VERSION_KEY);
                    for (String version : versionValues) {
                        if (!value.startsWith(version)) {
                            throw new HeaderException(
                                    String.format(
                                            "reader does not support the file type definition. line:%d [%s]",
                                            this.lineNumber, line));
                        }
                    }
                } else if (key.equals(TAXONOMYID_KEY)) {
                } else if (key.equals(RANKS_KEY)) {
                    if (!Profile.PRO_RANKS.equals(value.trim())) {
                        throw new HeaderException(
                                String.format(
                                        "Ranks field is incorrect on line:%d [%s]",
                                        this.lineNumber, line) + " CAMI only supports " + Profile.PRO_RANKS);
                    }
                } else if (!mandatoryFields.contains(key.toLowerCase()) && !key.matches("_[A-Za-z]*_[A-Za-z]+[A-Za-z0-9]*")) {
                    throw new HeaderException(
                            String.format(
                                    "reader does not support the file type definition. line:%d [%s]",
                                    this.lineNumber, line) + " Custom types MUST be prefixed " +
                                    "by a case-insensitive string with an underscore before and after the string ");
                }
//                else if (key.equals(entry.getKey())
//                             && !entry.getValue().contains(value) && ) {
//                    throw new HeaderException(
//                            String.format(
//                                    "reader does not support the file type definition. line:%d [%s]",
//                                    this.lineNumber, line));
//                }
            }
        }

        /**
         * Read the entire header record.
         *
         * @throws ParseException header was invalid in some manner
         * @throws IOException    error reading from file
         */
        protected void readHeader() throws ParseException, IOException {
            while (true) {
                String line = readLine();
                if (line == null) {
                    // should not have reached the end of the file here. This is
                    // an error.
                    throw new HeaderException(
                            String.format(
                                    "incomplete header/no column definition, file ended prematurely at line:%d",
                                    this.lineNumber));
                }

                // skip lines containing no information
                if (isBlank(line) || isComment(line)) {
                    continue;
                }
                // line containing the column names
                else if (isColumnsDef(line)) {
                    //cut the @@
                    String[] cols = line.substring(2).split(DELIMITER);

                    // implicit column definition as found in header.

                    this.columnDefinition = Arrays.asList(cols);

                    String[] columnDefArr = new String[this.columnDefinition.size()];

                    // else check that header agrees with explicit definition.
                    if (!Arrays.deepEquals(cols,
                            this.columnDefinition.toArray())) {
                        throw new HeaderException(
                                String.format(
                                        "column definition incorrect at line:%d %s should be %s",
                                        this.lineNumber, Arrays.toString(cols), Arrays.toString(columnDefinition.toArray(columnDefArr))));
                    }
                    // the column definition marks the end of the header
                    break;
                }
                // any header line
                else if (isHeaderLine(line)) {
                    parseHeaderLine(line);
                }
            }

            if (checkHeader) {
                checkMandatorySet();
            }
        }

        /**
         * Read a row of data from file.
         *
         * @return {@code String[]} containing the row in column order.
         * @throws ParseException invalid field number
         * @throws IOException    error while reading from file
         */
        public String[] readRow() throws ParseException, IOException {
            String line = readLine();
            if (line == null) {
                return null;
            }

            getLogger().debug("read: [{}]", line);

            if (isBlank(line) || isComment(line)) {
                return readRow();
            }

            String[] values = line.split(DELIMITER, -1);
            if (values.length != this.columnDefinition.size()) {
                throw new FieldException(String.format(
                        "'incorrect number of fields for line:%d [%s]'",
                        this.lineNumber, line));
            }

            return values;
        }

        /**
         * Get a specific header field from {@link #headerInfo}.
         *
         * @param key the field to fetch
         * @return value of header field
         * @throws HeaderException header field did not exist
         */
        public String getInfo(String key) throws HeaderException {
            if (!this.headerInfo.containsKey(key)) {
                throw new HeaderException(String.format("header did not contain a field %s", key));
            }
            return this.headerInfo.get(key);
        }


        public void clearHeaderInfo() {
            this.headerInfo.clear();
        }

        /**
         * Print the header information to the supplied output stream.
         *
         * @param os output stream to write
         * @throws IOException error with writing to output stream
         */
        public void printHeaderInfo(OutputStream os) throws IOException {
            BufferedWriter wr = null;
            try {
                wr = new BufferedWriter(new OutputStreamWriter(os));
                for (Map.Entry<String, String> entry : this.headerInfo
                        .entrySet()) {
                    wr.write(String.format("%s=%s\n", entry.getKey(),
                            entry.getValue()));
                }
            } finally {
                if (wr != null) {
                    wr.close();
                }
            }
        }

        /**
         * Close the underlying {@link java.io.BufferedReader}
         *
         * @throws IOException error with close the file.
         */
        public void close() throws IOException {
            if (this.reader != null) {
                this.reader.close();
            }
        }

        public Logger getLogger() {
            return logger;
        }

    }

    /**
     * Base exception for CamiIO
     */
    public static class ParseException extends Exception {
        private static final long serialVersionUID = 381788297090333090L;

        protected ParseException(String msg) {
            super(msg);
        }
    }

    /**
     * Header related exceptions
     */
    public static class HeaderException extends ParseException {
        private static final long serialVersionUID = -8296478832641545733L;

        protected HeaderException(String msg) {
            super(msg);
        }
    }

    /**
     * Data field related exceptions
     */
    public static class FieldException extends ParseException {
        private static final long serialVersionUID = -4882277234657138356L;

        public FieldException(String msg) {
            super(msg);
        }
    }

    /**
     * Parses a string either as double or int and returns the integer value.
     */
    public static Integer toInt(String str) throws NumberFormatException {
        int intTaxId;
        if (str.contains(".")) {
            intTaxId = (int) Double.parseDouble(str);
        } else {
            intTaxId = Integer.parseInt(str);
        }
        return intTaxId;
    }

    public static List<Integer> toIntList(String str, String delimiter) {
        List<Integer> intList = new ArrayList<>();
        if (str != null) {
            String[] tokens = str.split(delimiter);
            for (String s : tokens) {
                boolean isInt = true;
                try {
                    toInt(s);
                } catch (NumberFormatException e) {
                    isInt = false;
                }
                if (isInt) {
                    intList.add((int) Double.parseDouble(s));
                }
            }
        }
        return intList;
    }
}
