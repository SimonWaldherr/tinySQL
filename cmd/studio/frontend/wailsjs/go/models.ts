export namespace main {
	
	export class ColumnInfo {
	    name: string;
	    type: string;
	
	    static createFrom(source: any = {}) {
	        return new ColumnInfo(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.name = source["name"];
	        this.type = source["type"];
	    }
	}
	export class ImportResponse {
	    success: boolean;
	    tableName?: string;
	    rowsImported?: number;
	    rowsSkipped?: number;
	    columns?: string[];
	    warnings?: string[];
	    delimiter?: string;
	    hadHeader?: boolean;
	    error?: string;
	
	    static createFrom(source: any = {}) {
	        return new ImportResponse(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.success = source["success"];
	        this.tableName = source["tableName"];
	        this.rowsImported = source["rowsImported"];
	        this.rowsSkipped = source["rowsSkipped"];
	        this.columns = source["columns"];
	        this.warnings = source["warnings"];
	        this.delimiter = source["delimiter"];
	        this.hadHeader = source["hadHeader"];
	        this.error = source["error"];
	    }
	}
	export class QueryResult {
	    columns: string[];
	    rows: any[][];
	    error?: string;
	    message?: string;
	    count: number;
	    elapsed_ms: number;
	
	    static createFrom(source: any = {}) {
	        return new QueryResult(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.columns = source["columns"];
	        this.rows = source["rows"];
	        this.error = source["error"];
	        this.message = source["message"];
	        this.count = source["count"];
	        this.elapsed_ms = source["elapsed_ms"];
	    }
	}
	export class TableInfo {
	    name: string;
	    columns: ColumnInfo[];
	    rowCount: number;
	
	    static createFrom(source: any = {}) {
	        return new TableInfo(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.name = source["name"];
	        this.columns = this.convertValues(source["columns"], ColumnInfo);
	        this.rowCount = source["rowCount"];
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}

}

