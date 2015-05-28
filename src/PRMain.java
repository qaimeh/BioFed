import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.MalformedInputException;

import org.openjena.riot.RIOT;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.vocabulary.RDF;

public class PRMain {

	private static final long FIRST_RESULT_TIMEOUT = 150000;
	private static final long EXECUTION_TIMEOUT = 10 * 60 * 1000;

	// create a singleton instance

	private static PRMain instance = null;

	protected PRMain() {

	}

	public static PRMain getInstance() {
		if (instance == null) {
			instance = new PRMain();
		}
		return instance;
	}

	public static void main(String[] args) {
		Model model = ModelFactory.createDefaultModel();
		InputStream in = FileManager.get().open("smalldata.n3");
		if ( in == null) {
			throw new IllegalArgumentException("File: " + " not found");
		}

		RIOT.init();
		model.read( in , null, "N3");
		String qString = "SELECT distinct ?subject WHERE { ?subject a <http://rdfs.org/ns/void#Dataset>.}";
		// String remoteString = "SELECT distinct ?P { ?S ?P ?O. }";

		String remoteString = "PREFIX dcat: <http://www.w3.org/ns/dcat#>" + "PREFIX dcterms: <http://purl.org/dc/terms/>" + "PREFIX prov: <http://www.w3.org/ns/prov#>  " + "PREFIX void: <http://rdfs.org/ns/void#>  " + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>  " +

			"SELECT * { ?dataset rdf:type dcterms:Dataset ." + "?dataset dcat:distribution ?dist ." + "?dist ?p ?o ." + "} order by ?dataset ?dist ?p ?o";

		String dctermsSub = "SELECT distinct ?subject WHERE { ?subject a <http://purl.org/dc/terms/Dataset>.}";
		// String namespace= "http://rdfs.org/ns/void#";

		String namespace = "http://purl.org/dc/terms/";
		String distribution = "http://www.w3.org/ns/dcat#";
		Resource x = null, dcX = null;

		Query query = QueryFactory.create(qString);

		QueryExecution qexec = QueryExecutionFactory.create(query, model);

		ResultSet results = qexec.execSelect();
		for (; results.hasNext();) {
			try {
				QuerySolution soln = results.nextSolution();
				x = soln.get("subject").asResource();
				if (isEndpointAlive(x.toString())) {

					Property prp = model.createProperty(namespace, "Dataset");
					x.addProperty(prp, x + "#ProvInfo");

					if (x.hasProperty(prp)) {

						RDFNode obj = x.getProperty(prp).getObject();

						model.createResource(obj.toString(), prp);
					}

					// String dctermsSub=
					// String.format("SELECT * WHERE { <%s> a <http://purl.org/dc/terms/Dataset>.}",x.getProperty(prp).getObject());

					Query dctermQry = QueryFactory.create(dctermsSub);

					// Query dctermQry= QueryFactory.create(dctermsSub);
					QueryExecution dctermsExec = QueryExecutionFactory.create(
					dctermQry, model);

					ResultSet dctermsResults = dctermsExec.execSelect();

					for (; dctermsResults.hasNext();) {

						QuerySolution dcsoln = dctermsResults.nextSolution();

						// get endpoint resource as provenance
						dcX = dcsoln.get("subject").asResource();

						String getDataset = "select distinct ?subject { ?subject a <http://purl.org/dc/terms/Dataset>}";
						ResultSet rsDataset = queryEndpoint(x.toString(),
						getDataset);

						for (; rsDataset.hasNext();) {
							QuerySolution solDataset = rsDataset.nextSolution();

							Resource rs = solDataset.get("subject")
								.asResource();
							// Property prp= model.createProperty(namespace,
							// "Dataset");

							// Property
							// dataPr=model.createProperty(rs.asResource().getNameSpace());
							dcX.addProperty(prp, rs);

							if (dcX.hasProperty(prp)) {
								RDFNode obj = dcX.getProperty(prp).getObject();
								model.createResource(obj.toString(), prp);

								// query distributions
								Property distributionPrp = model.createProperty(distribution,
									"distribution");
								String getDistribution = String.format("SELECT distinct ?distribution {<%s> a <%s>. <%s> <%s> ?distribution . }",
								obj.toString(), prp,
								obj.toString(), distributionPrp);

								ResultSet rsDistribution = queryEndpoint(
								x.toString(), getDistribution);

								for (; rsDistribution.hasNext();) {
									QuerySolution solDistribution = rsDistribution.nextSolution();
									RDFNode distrib = solDistribution.get("distribution");

									//rs.addProperty(distributionPrp, distrib);
									model.add(rs, distributionPrp, distrib);
									//model.createResource(distrib.toString(), distributionPrp);
									System.err.println(distrib);

									String getDistributionSPO = String.format("SELECT * {<%s> ?p ?o . }", distrib);

									ResultSet rsDistributionSPO = queryEndpoint(
									x.toString(), getDistributionSPO);


									for (; rsDistributionSPO.hasNext();) {

										QuerySolution solDistrSPO = rsDistributionSPO.nextSolution();

										Property namespc = model.createProperty(solDistrSPO.get("p").toString());

										RDFNode object = solDistrSPO.get("o");

										model.add(distrib.asResource(), namespc, object);
										//model.createProperty(rs.asResource().getNameSpace());

										System.err.println(solDistrSPO);
									}



								}

								// System.err.println(getDistribution);
							}

						}

						/*
						 * QueryExecution qExec =
						 * QueryExecutionFactory.sparqlService(x.toString(),
						 * remoteString); qExec.setTimeout(FIRST_RESULT_TIMEOUT,
						 * EXECUTION_TIMEOUT); ResultSet result =
						 * qExec.execSelect(); while(result.hasNext()){
						 * System.out.println("adding property"); QuerySolution
						 * qs = result.next();
						 * System.err.println(qs.get("dataset"));
						 * System.err.println(qs.get("dist"));
						 * System.err.println(qs.get("p"));
						 * System.err.println(qs.get("o")); // Property p =
						 * model
						 * .createProperty(namspace,"additionalProperties");
						 * 
						 * // dcX.addLiteral(p, qs.get("o")); }
						 */

					}

				}
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println(e.getMessage());
				System.out.println(x);

			}
		}
		try {
			model.write(new FileWriter("updated-new-data.n3"), "N3");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.err.println(x);

		}

	}

	private static ResultSet queryEndpoint(String Url, String query) {

		Query qry = QueryFactory.create(query);
		QueryExecution qExec = QueryExecutionFactory.sparqlService(Url, qry);
		qExec.setTimeout(FIRST_RESULT_TIMEOUT, EXECUTION_TIMEOUT);
		ResultSet result = qExec.execSelect();
		return result;

	}

	private static ResultSet queryModel(String query, Model mdl) {
		Query qry = QueryFactory.create(query);
		QueryExecution qExec = QueryExecutionFactory.create(qry, mdl);

		ResultSet result = qExec.execSelect();
		return result;

	}

	private static boolean isEndpointAlive(String endpoint) {

		try {
			String query = "SELECT ?s WHERE { ?s ?p ?o . } LIMIT 1";

			QueryExecution qExc = QueryExecutionFactory.sparqlService(endpoint,
			query);
			qExc.setTimeout(FIRST_RESULT_TIMEOUT, EXECUTION_TIMEOUT);
			ResultSet rs = qExc.execSelect();

			if (rs != null) {
				return true;
			} else {
				return false;
			}
		} catch (Exception e) {
			return false;
		}
	}

}
