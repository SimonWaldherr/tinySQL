package importer

// GeoPackageInfo describes the standard metadata needed to choose a feature
// layer without importing it. It contains no service/provider-specific fields.
type GeoPackageInfo struct {
	ApplicationID int64
	UserVersion   int
	Version       string
	Layers        []GeoPackageLayer
}

// GeoPackageLayer is one OGC GeoPackage vector feature table discovered via
// gpkg_contents, gpkg_geometry_columns, and gpkg_spatial_ref_sys.
type GeoPackageLayer struct {
	TableName         string
	Identifier        string
	Description       string
	LastChange        string
	GeometryColumn    string
	GeometryType      string
	SRID              int64
	Z                 int
	M                 int
	MinX              *float64
	MinY              *float64
	MaxX              *float64
	MaxY              *float64
	SRSName           string
	SRSOrganization   string
	SRSOrganizationID int64
	SRSDefinition     string
}
