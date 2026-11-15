// Package cloudquery hosts CloudQuery source plugins as a VGI worker: each
// plugin named at ATTACH becomes a schema of the catalog and each plugin
// table a table.
//
//	ATTACH 'cloudquery' AS cq (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-cloudquery-worker',
//	    plugins 'github:cloudquery/test@v4.1.0');
//	SELECT * FROM cq.test.test_some_table;
package cloudquery

import (
	"github.com/Query-farm/vgi-go/vgi"
)

func allExamples() []exampleQuery {
	return exampleQueries(scanExamples, installExamples, pluginsDocs.Examples, pluginTablesDocs.Examples, pluginColumnsDocs.Examples)
}

// NewWorker builds the worker. Extra options (logging, storage) are appended
// after the catalog options.
func NewWorker(opts ...vgi.WorkerOption) *vgi.Worker {
	sourceURL, version := SourceURL, Version
	base := []vgi.WorkerOption{
		vgi.WithCatalogName(CatalogName),
		vgi.WithCatalogComment(catalogComment),
		vgi.WithCatalogTags(CatalogTags),
		vgi.WithCatalogInfo(vgi.CatalogInfo{SourceURL: &sourceURL, ImplementationVersion: &version}),
		vgi.WithSchemaComments(map[string]string{"main": SchemaDocMD}),
		vgi.WithSchemaTags(map[string]map[string]string{"main": schemaTags(allExamples())}),
		vgi.WithSecretTypes(PluginSecretType, HubSecretSpec),
		vgi.WithAttachOptions(AttachOptionSpecs...),
		vgi.WithAttachValidator(validateAttach),
		vgi.WithCatalogContentsHandler(catalogContents),
		vgi.WithSchemaContentsHandler(schemaTables),
		vgi.WithAttachTableGetHandler(tableGet),
		vgi.WithAttachScanFunctionGetHandler(scanFunctionGet),
	}
	w := vgi.NewWorker(append(base, opts...)...)
	w.RegisterTable(vgi.AsTableFunction[ScanState](&Scan{}))
	w.RegisterTable(vgi.AsTableFunction[InstallState](&Install{}))
	for _, t := range NewInfoTables() {
		w.RegisterCatalogTable("main", t)
	}
	return w
}
