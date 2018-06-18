package cami.io.concat;

import cami.io.Base;
import cami.io.Profile;
import mzd.taxonomy.neo.NeoDao;
import java.io.IOException;

import static cami.io.Base.*;


public class ConcatProfilingIter extends Profile.ValidatingReader {

    public ConcatProfilingIter(String fileName, String neoDBPath, Boolean checkHeader) throws Base.ParseException, IOException {
        super(fileName, neoDBPath, checkHeader);
    }

    public ConcatProfilingIter(String fileName, NeoDao neoDao,
                            Boolean checkHeader) throws ParseException, IOException {
        super(fileName, neoDao, checkHeader);
    }

    @Override
    public String[] readRow() throws Base.ParseException, IOException {
        String line = readLine();
        if (line == null) {
            return null;
        }

        getLogger().debug("read: [{}]", line);

        if (isBlank(line) || isComment(line)) {
            return readRow();
        }

        //next profiling data started?
        if(isHeaderLine(line)){
            clearHeaderInfo();
            parseHeaderLine(line);
            readHeader();
            return readRow();
        }

        String[] values = line.split(Base.DELIMITER,-1);
        if (values.length != this.columnDefinition.size()) {
            throw new Base.FieldException(String.format(
                    "'incorrect number of fields for line:%d [%s]'",
                    this.lineNumber, line));
        }

        if (values == null) {
            // end of file reached
            return null;
        }
        checkInvalidRank(values[1],this.lineNumber);
        int taxId = toInt(values[0]);
        checkInvalidTAXID(taxId);
        checkInvalidTaxPath(values[2]);
        checkInvalidPercentageNumber(values[percentageColumn]);

        return values;
    }

    private void checkInvalidPercentageNumber(String percentage) throws FieldException {
        try{
            Double.parseDouble(percentage);
        } catch (NumberFormatException ex){
            throw new Base.FieldException(String.format(
                    "'invalid PERCENTAGE number in line:%d [%s]'",
                    lineNumber, percentage));
        }
    }

    private void checkInvalidRank(String value, int linenumber) throws FieldException {
        if(!Profile.PRO_RANKS.contains(value)){
            throw new Base.FieldException(String.format(
                    "'invalid rank in line:%d [%s]'",
                    lineNumber, value));
        }
    }
}
