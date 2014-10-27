/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
module presto.odbcdriver.prestoresults;

import std.array : front, empty, popFront;
import std.conv : text, to;
import std.variant : Variant;
import facebook.json : JSONValue, JSON_TYPE;

import odbc.sqlext;
import odbc.odbcinst;

import presto.client.queryresults : ColumnMetadata;

import presto.odbcdriver.bindings : OdbcResult, OdbcResultRow;
import presto.odbcdriver.util : dllEnforce, logMessage, makeWithoutGC;

import presto.client.statementclient : StatementClient;

void addToPrestoResultRow(JSONValue columnData, PrestoResultRow result, string resultDataType) {
    final switch (columnData.type) {
        case JSON_TYPE.STRING:
            if (resultDataType == "varchar") {
                result.addNextValue(Variant(columnData.str));
            } else {
                if (columnData.str != "NaN") { 
                    dllEnforce(false, "A non-Nan double is not expected in the result");
                } 
                result.addNextValue(Variant(double.nan));
            }
            break;
        case JSON_TYPE.INTEGER:
            result.addNextValue(Variant(columnData.integer));
            break;
        case JSON_TYPE.FLOAT:
            result.addNextValue(Variant(columnData.floating));
            break;
        case JSON_TYPE.TRUE:
            result.addNextValue(Variant(true));
            break;
        case JSON_TYPE.FALSE:
            result.addNextValue(Variant(false));
            break;
        case JSON_TYPE.NULL:
            result.addNextValue(Variant(null));
            break;
        case JSON_TYPE.OBJECT:
        case JSON_TYPE.ARRAY:
        case JSON_TYPE.UINTEGER:
            dllEnforce(false, "Unexpected JSON type: " ~ text(columnData.type));
            break;
    }
}

unittest {
    JSONValue v = "NaN";
    auto r = new PrestoResultRow();
    addToPrestoResultRow(v, r, "double");
    assert(r.data[0].type == typeid(double));//double NaN
    addToPrestoResultRow(v, r, "varchar");//string "NaN"
    assert(r.data[1].type == typeid(string));
    v = "{\"x\":\"y\"}";
    addToPrestoResultRow(v, r, "varchar");
    assert(r.data[2].type == typeid(string));
}

final class PrestoResult : OdbcResult {
    
    this(StatementClient client){
        this.client_ = client; 
        preparePageData();    
    }

    void addRow(PrestoResultRow r) {
        dllEnforce(r.numberOfColumns() != 0, "Row has at least 1 column");
        results_ ~= r;
        numberOfColumns_ = r.numberOfColumns();
    }

    override bool empty() const {
        return results_.empty() && client_.empty();
    }

    override inout(PrestoResultRow) front() inout {
        logMessage("-------------front----------------");
        logMessage(results_.length);

        assert(!empty);
        return results_.front();
    }

    override void popFront() {
        results_.popFront();
        //  prepare next range data from client.
        if(results_.empty() && !client_.empty()){
            logMessage("=======get more data=========");
            preparePageData();
        }
    }

    override size_t numberOfColumns() {
        return numberOfColumns_;
    }

    void columnMetadata(immutable(ColumnMetadata)[] data) {
        if (!columnMetadata_) {
            columnMetadata_ = data;
        }
    }

    auto columnMetadata() const {
        return columnMetadata_;
    }

private:
    StatementClient client_;
    PrestoResultRow[] results_;
    immutable(ColumnMetadata)[] columnMetadata_ = null;
    size_t numberOfColumns_ = 0;

    void preparePageData(){
        logMessage("=======preparePageData=========");

        auto resultBatch = client_.front();
        columnMetadata(resultBatch.columnMetadata);
        foreach (ref row; resultBatch.data.array) {
            auto dataRow = makeWithoutGC!PrestoResultRow();
            foreach (i, ref columnData; row.array) {
                addToPrestoResultRow(columnData, dataRow, columnMetadata_[i].type);
            }
            dllEnforce(dataRow.numberOfColumns() != 0, "Row has at least 1 column");
            addRow(dataRow);
        }
        logMessage("results_.length = ", results_.length);
        if(!client_.empty()){
            client_.popFront();   
        }
        logMessage("nextURI--> ", resultBatch.nextURI);
        logMessage("resultBatch.data.array.length--> ", resultBatch.data.array.length);
        if(resultBatch.nextURI != "" && resultBatch.data.array.length == 0){
            logMessage("No data in this request, try again..");
            preparePageData();
        }
    }
}

final class PrestoResultRow : OdbcResultRow {
    void addNextValue(Variant v) {
        data ~= v;
    }

    override Variant dataAt(int column) {
        dllEnforce(column >= 1);
        return data[column - 1];
    }

    size_t numberOfColumns() {
        dllEnforce(!data.empty, "Row has 0 columns");
        return cast(uint) data.length;
    }
private:
    Variant[] data;
}
