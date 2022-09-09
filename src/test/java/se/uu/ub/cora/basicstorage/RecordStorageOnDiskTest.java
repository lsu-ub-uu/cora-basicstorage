/*
 * Copyright 2016, 2018 Olov McKie
 * Copyright 2016, 2018, 2019 Uppsala University Library
 *
 * This file is part of Cora.
 *
 *     Cora is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     Cora is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with Cora.  If not, see <http://www.gnu.org/licenses/>.
 */
package se.uu.ub.cora.basicstorage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import se.uu.ub.cora.basicdata.converter.datatojson.BasicDataToJsonConverterFactoryCreator;
import se.uu.ub.cora.basicdata.converter.datatojson.DataGroupToJsonConverter;
import se.uu.ub.cora.basicdata.converter.jsontodata.JsonToDataConverterFactoryImp;
import se.uu.ub.cora.basicstorage.testdata.DataCreator;
import se.uu.ub.cora.data.DataAtomicFactory;
import se.uu.ub.cora.data.DataAtomicProvider;
import se.uu.ub.cora.data.DataGroup;
import se.uu.ub.cora.data.DataGroupFactory;
import se.uu.ub.cora.data.DataGroupProvider;
import se.uu.ub.cora.data.collectterms.StorageTerm;
import se.uu.ub.cora.data.converter.DataToJsonConverterFactory;
import se.uu.ub.cora.data.converter.DataToJsonConverterProvider;
import se.uu.ub.cora.data.converter.JsonToDataConverterFactory;
import se.uu.ub.cora.data.converter.JsonToDataConverterProvider;
import se.uu.ub.cora.data.copier.DataCopierFactory;
import se.uu.ub.cora.data.copier.DataCopierProvider;
import se.uu.ub.cora.storage.RecordNotFoundException;

public class RecordStorageOnDiskTest {
	private static final String PLACE_JSCLIENT_FILENAME = "place_jsClient.json.gz";
	private static final String PERSON_FILENAME = "person_cora.json.gz";
	private static final String PLACE_CORA_FILENAME = "place_cora.json.gz";
	private static final String COLLECTED_DATA_FILENAME = "collectedData_cora.json.gz";
	private static final String LINK_LISTS_FILENAME = "linkLists_cora.json.gz";
	private static final String FROM_RECORD_TYPE = "fromRecordType";
	private static final String TO_RECORD_ID = "toRecordId";
	private static final String TO_RECORD_TYPE = "toRecordType";
	private String basePath = "/tmp/recordStorageOnDiskTemp/";
	private DataGroup emptyLinkList = DataCreator.createEmptyLinkList();
	private DataGroupFactory dataGroupFactory;
	private DataAtomicFactory dataAtomicFactory;
	private DataCopierFactory dataCopierFactory;
	private DataToJsonConverterFactory dataToJsonConverterFactory;
	private JsonToDataConverterFactory jsonToDataConverterFactory;
	List<StorageTerm> storageTerms = Collections.emptyList();

	private String expectedRecordJsonOneRecordPlace1 = getExpectedRecordJsonOneRecordPlace1();

	private String getExpectedRecordJsonOneRecordPlace1() {
		String expectedJson = "{\n";
		expectedJson += "    \"children\": [{\n";
		expectedJson += "        \"children\": [{\n";
		expectedJson += "            \"children\": [\n";
		expectedJson += "                {\n";
		expectedJson += "                    \"name\": \"type\",\n";
		expectedJson += "                    \"value\": \"place\"\n";
		expectedJson += "                },\n";
		expectedJson += "                {\n";
		expectedJson += "                    \"name\": \"id\",\n";
		expectedJson += "                    \"value\": \"place:0001\"\n";
		expectedJson += "                },\n";
		expectedJson += "                {\n";
		expectedJson += "                    \"children\": [\n";
		expectedJson += "                        {\n";
		expectedJson += "                            \"name\": \"linkedRecordType\",\n";
		expectedJson += "                            \"value\": \"system\"\n";
		expectedJson += "                        },\n";
		expectedJson += "                        {\n";
		expectedJson += "                            \"name\": \"linkedRecordId\",\n";
		expectedJson += "                            \"value\": \"cora\"\n";
		expectedJson += "                        }\n";
		expectedJson += "                    ],\n";
		expectedJson += "                    \"name\": \"dataDivider\"\n";
		expectedJson += "                }\n";
		expectedJson += "            ],\n";
		expectedJson += "            \"name\": \"recordInfo\"\n";
		expectedJson += "        }],\n";
		expectedJson += "        \"name\": \"authority\"\n";
		expectedJson += "    }],\n";
		expectedJson += "    \"name\": \"recordList\"\n";
		expectedJson += "}\n";
		return expectedJson;
	}

	private String expectedRecordJsonOneRecordPlace2 = "{\"children\":[{\"children\":[{\"children\":[{\"name\":\"type\""
			+ ",\"value\":\"place\"}" + ",{\"name\":\"id\",\"value\":\"place:0002\"}"
			+ ",{\"children\":[{\"name\":\"linkedRecordType\",\"value\":\"system\"}"
			+ ",{\"name\":\"linkedRecordId\",\"value\":\"cora\"}],\"name\":\"dataDivider\"}]"
			+ ",\"name\":\"recordInfo\"}" + "],\"name\":\"authority\"}],\"name\":\"recordList\"}";

	private String expectedRecordJsonTwoRecords = getExpectedRecordJsonTwoRecords();
	private RecordStorageOnDisk recordStorage;

	private String getExpectedRecordJsonTwoRecords() {
		String expectedJson = "{\n";
		expectedJson += "    \"children\": [\n";
		expectedJson += "        {\n";
		expectedJson += "            \"children\": [{\n";
		expectedJson += "                \"children\": [\n";
		expectedJson += "                    {\n";
		expectedJson += "                        \"name\": \"type\",\n";
		expectedJson += "                        \"value\": \"place\"\n";
		expectedJson += "                    },\n";
		expectedJson += "                    {\n";
		expectedJson += "                        \"name\": \"id\",\n";
		expectedJson += "                        \"value\": \"place:0001\"\n";
		expectedJson += "                    },\n";
		expectedJson += "                    {\n";
		expectedJson += "                        \"children\": [\n";
		expectedJson += "                            {\n";
		expectedJson += "                                \"name\": \"linkedRecordType\",\n";
		expectedJson += "                                \"value\": \"system\"\n";
		expectedJson += "                            },\n";
		expectedJson += "                            {\n";
		expectedJson += "                                \"name\": \"linkedRecordId\",\n";
		expectedJson += "                                \"value\": \"cora\"\n";
		expectedJson += "                            }\n";
		expectedJson += "                        ],\n";
		expectedJson += "                        \"name\": \"dataDivider\"\n";
		expectedJson += "                    }\n";
		expectedJson += "                ],\n";
		expectedJson += "                \"name\": \"recordInfo\"\n";
		expectedJson += "            }],\n";
		expectedJson += "            \"name\": \"authority\"\n";
		expectedJson += "        },\n";
		expectedJson += "        {\n";
		expectedJson += "            \"children\": [{\n";
		expectedJson += "                \"children\": [\n";
		expectedJson += "                    {\n";
		expectedJson += "                        \"name\": \"type\",\n";
		expectedJson += "                        \"value\": \"place\"\n";
		expectedJson += "                    },\n";
		expectedJson += "                    {\n";
		expectedJson += "                        \"name\": \"id\",\n";
		expectedJson += "                        \"value\": \"place:0002\"\n";
		expectedJson += "                    },\n";
		expectedJson += "                    {\n";
		expectedJson += "                        \"children\": [\n";
		expectedJson += "                            {\n";
		expectedJson += "                                \"name\": \"linkedRecordType\",\n";
		expectedJson += "                                \"value\": \"system\"\n";
		expectedJson += "                            },\n";
		expectedJson += "                            {\n";
		expectedJson += "                                \"name\": \"linkedRecordId\",\n";
		expectedJson += "                                \"value\": \"cora\"\n";
		expectedJson += "                            }\n";
		expectedJson += "                        ],\n";
		expectedJson += "                        \"name\": \"dataDivider\"\n";
		expectedJson += "                    }\n";
		expectedJson += "                ],\n";
		expectedJson += "                \"name\": \"recordInfo\"\n";
		expectedJson += "            }],\n";
		expectedJson += "            \"name\": \"authority\"\n";
		expectedJson += "        }\n";
		expectedJson += "    ],\n";
		expectedJson += "    \"name\": \"recordList\"\n";
		expectedJson += "}\n";
		return expectedJson;
	}

	@BeforeMethod
	public void makeSureBasePathExistsAndIsEmpty() throws IOException {
		File dir = new File(basePath);
		dir.mkdir();
		deleteFiles(basePath);
		dataGroupFactory = new DataGroupFactorySpy();
		DataGroupProvider.setDataGroupFactory(dataGroupFactory);
		dataAtomicFactory = new DataAtomicFactorySpy();
		DataAtomicProvider.setDataAtomicFactory(dataAtomicFactory);
		dataCopierFactory = new DataCopierFactorySpy();
		DataCopierProvider.setDataCopierFactory(dataCopierFactory);
		// dataToJsonConverterFactory = new DataToJsonConverterFactoryImp();
		// DataToJsonConverterProvider.setDataToJsonConverterFactory(dataToJsonConverterFactory);
		BasicDataToJsonConverterFactoryCreator factoryCreator = new BasicDataToJsonConverterFactoryCreator();
		DataToJsonConverterProvider.setDataToJsonConverterFactoryCreator(factoryCreator);
		jsonToDataConverterFactory = new JsonToDataConverterFactoryImp();
		JsonToDataConverterProvider.setJsonToDataConverterFactory(jsonToDataConverterFactory);

		recordStorage = RecordStorageOnDisk.createRecordStorageOnDiskWithBasePath(basePath);
	}

	private void createRecordTypePlace() {
		DataGroup placeRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("place", "true", "false");
		recordStorage.create("recordType", "place", placeRecordType, storageTerms, emptyLinkList,
				"cora");
	}

	private void deleteFiles(String path) throws IOException {
		Stream<Path> list;
		list = Files.list(Paths.get(path));

		list.forEach(p -> deleteFile(p));
		list.close();
	}

	private void deleteFile(Path path) {
		try {
			if (path.toFile().isDirectory()) {
				deleteFiles(path.toString());
			}
			Files.delete(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@AfterMethod
	public void removeTempFiles() throws IOException {
		if (Files.exists(Paths.get(basePath))) {
			deleteFiles(basePath);
			File dir = new File(basePath);
			dir.delete();
		}
	}

	@Test
	public void testGetBasePath() throws IOException {
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);
		assertEquals(recordStorage.getBasePath(), basePath);
	}

	@Test
	public void testInitNoFilesOnDisk() throws IOException {
		DataGroup emptyLinkList = new DataGroupSpy("collectedDataLinks");
		createRecordTypePlace();

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");
		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		assertJsonEqualDataGroup(dataGroupOut, dataGroup);

		assertEquals(readJsonFileFromDisk(PLACE_CORA_FILENAME, "cora"),
				expectedRecordJsonOneRecordPlace1);
		Path path = Paths.get(basePath, LINK_LISTS_FILENAME);
		assertFalse(Files.exists(path));
	}

	private DataGroup createDataGroupWithRecordInfo() {
		return DataCreator.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId(
				"authority", "place", "place:0001");
	}

	@Test
	public void testInitNoFilesOnDiskTwoSystems() throws IOException {
		DataGroup emptyLinkList = new DataGroupSpy("collectedDataLinks");
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");
		assertEquals(readJsonFileFromDisk(PLACE_CORA_FILENAME, "cora"),
				expectedRecordJsonOneRecordPlace1);

		recordStorage.create("place", "place:0002", dataGroup, storageTerms, emptyLinkList,
				"jsClient");
		assertEquals(readJsonFileFromDisk(PLACE_JSCLIENT_FILENAME, "jsClient"),
				expectedRecordJsonOneRecordPlace1);
	}

	private String readJsonFileFromDisk(String fileName, String dataDivider) throws IOException {
		Path path = Paths.get(basePath + "/" + dataDivider, fileName);

		InputStream newInputStream = Files.newInputStream(path);
		InputStreamReader inputStreamReader = new java.io.InputStreamReader(
				new GZIPInputStream(newInputStream), "UTF-8");
		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
		String line = null;
		String json = "";
		while ((line = bufferedReader.readLine()) != null) {
			json += line + "\n";
		}
		bufferedReader.close();

		return json;
	}

	@Test
	public void testInitNoFilesOnDiskTwoSystemsMoveRecordBetweenSystems() throws IOException {
		DataGroup emptyLinkList = new DataGroupSpy("collectedDataLinks");
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");
		assertEquals(readJsonFileFromDisk(PLACE_CORA_FILENAME, "cora"),
				expectedRecordJsonOneRecordPlace1);

		recordStorage.create("place", "place:0002", dataGroup, storageTerms, emptyLinkList,
				"jsClient");
		assertEquals(readJsonFileFromDisk(PLACE_JSCLIENT_FILENAME, "jsClient"),
				expectedRecordJsonOneRecordPlace1);

		DataGroup dataGroup2 = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("authority",
						"place", "place:0002");
		recordStorage.update("place", "place:0002", dataGroup2, storageTerms, emptyLinkList,
				"cora");
		assertEquals(readJsonFileFromDisk(PLACE_CORA_FILENAME, "cora"),
				expectedRecordJsonTwoRecords);

		Path path = Paths.get(basePath, "jsClient", PLACE_JSCLIENT_FILENAME);
		assertFalse(Files.exists(path));

		Path pathIncludingDataDivider = Paths.get(basePath, "jsClient");
		assertFalse(pathIncludingDataDivider.toFile().exists());
	}

	@Test
	public void testInitTwoFilesOnDiskTwoSystems() throws IOException {
		createRecordTypePlace();
		writeZippedFileToDisk(expectedRecordJsonOneRecordPlace1, "cora", PLACE_CORA_FILENAME);
		writeZippedFileToDisk(expectedRecordJsonOneRecordPlace2, "cora", "place_jsClient.json");

		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		assertJsonEqualDataGroup(dataGroupOut, dataGroup);

		DataGroup dataGroup2 = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("authority",
						"place", "place:0002");
		DataGroup dataGroupOut2 = recordStorage.read("place", "place:0002");
		assertJsonEqualDataGroup(dataGroupOut2, dataGroup2);
	}

	@Test
	public void testInitTwoFilesOnDiskOneZippedOneUnzipped() throws IOException {
		createRecordTypePlace();
		writeZippedFileToDisk(expectedRecordJsonOneRecordPlace1, "cora", PLACE_CORA_FILENAME);
		writeFileToDisk(expectedRecordJsonOneRecordPlace2, "cora", "place_jsClient.json");

		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		assertJsonEqualDataGroup(dataGroupOut, dataGroup);

		DataGroup dataGroup2 = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("authority",
						"place", "place:0002");
		DataGroup dataGroupOut2 = recordStorage.read("place", "place:0002");
		assertJsonEqualDataGroup(dataGroupOut2, dataGroup2);
	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	public void testInitStreamsFolderShouldNotBeRead() throws IOException {
		createRecordTypePlace();
		writeFileToDisk(expectedRecordJsonOneRecordPlace1, "streams", PLACE_CORA_FILENAME);
		writeFileToDisk(expectedRecordJsonOneRecordPlace2, "streams", "place_jsClient.json");

		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		recordStorage.read("place", "place:0001");
	}

	@Test
	public void testInitTwoFilesOnDiskTwoSystemsUnrelatedDirectory() throws IOException {
		createRecordTypePlace();
		writeDirectoryToDisk("someUnrelatedDir");
		writeZippedFileToDisk(expectedRecordJsonOneRecordPlace1, "cora", PLACE_CORA_FILENAME);
		writeZippedFileToDisk(expectedRecordJsonOneRecordPlace2, "jsClient",
				PLACE_JSCLIENT_FILENAME);

		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		assertJsonEqualDataGroup(dataGroupOut, dataGroup);

		DataGroup dataGroup2 = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("authority",
						"place", "place:0002");
		DataGroup dataGroupOut2 = recordStorage.read("place", "place:0002");
		assertJsonEqualDataGroup(dataGroupOut2, dataGroup2);
	}

	@Test
	public void testRecordWithLinks() throws IOException {
		createRecordTypePlace();
		DataGroup linkListWithTwoLinks = createLinkListWithTwoLinks("place:0001");
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, storageTerms, linkListWithTwoLinks,
				"cora");
		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		assertJsonEqualDataGroup(dataGroupOut, dataGroup);

		assertEquals(readJsonFileFromDisk(PLACE_CORA_FILENAME, "cora"),
				expectedRecordJsonOneRecordPlace1);

		String expectedLinkListJson = "{\n";
		expectedLinkListJson += "    \"children\": [{\n";
		expectedLinkListJson += "        \"children\": [{\n";
		expectedLinkListJson += "            \"children\": [{\n";
		expectedLinkListJson += "                \"children\": [\n";
		expectedLinkListJson += "                    {\n";
		expectedLinkListJson += "                        \"children\": [\n";
		expectedLinkListJson += "                            {\n";
		expectedLinkListJson += "                                \"children\": [\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                        \"value\": \"fromRecordType\"\n";
		expectedLinkListJson += "                                    },\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                        \"value\": \"place:0001\"\n";
		expectedLinkListJson += "                                    }\n";
		expectedLinkListJson += "                                ],\n";
		expectedLinkListJson += "                                \"name\": \"from\"\n";
		expectedLinkListJson += "                            },\n";
		expectedLinkListJson += "                            {\n";
		expectedLinkListJson += "                                \"children\": [\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                        \"value\": \"toRecordType\"\n";
		expectedLinkListJson += "                                    },\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                        \"value\": \"toRecordId\"\n";
		expectedLinkListJson += "                                    }\n";
		expectedLinkListJson += "                                ],\n";
		expectedLinkListJson += "                                \"name\": \"to\"\n";
		expectedLinkListJson += "                            }\n";
		expectedLinkListJson += "                        ],\n";
		expectedLinkListJson += "                        \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson += "                    },\n";
		expectedLinkListJson += "                    {\n";
		expectedLinkListJson += "                        \"children\": [\n";
		expectedLinkListJson += "                            {\n";
		expectedLinkListJson += "                                \"children\": [\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                        \"value\": \"fromRecordType\"\n";
		expectedLinkListJson += "                                    },\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                        \"value\": \"place:0001\"\n";
		expectedLinkListJson += "                                    }\n";
		expectedLinkListJson += "                                ],\n";
		expectedLinkListJson += "                                \"name\": \"from\"\n";
		expectedLinkListJson += "                            },\n";
		expectedLinkListJson += "                            {\n";
		expectedLinkListJson += "                                \"children\": [\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                        \"value\": \"toRecordType\"\n";
		expectedLinkListJson += "                                    },\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                        \"value\": \"toRecordId2\"\n";
		expectedLinkListJson += "                                    }\n";
		expectedLinkListJson += "                                ],\n";
		expectedLinkListJson += "                                \"name\": \"to\"\n";
		expectedLinkListJson += "                            }\n";
		expectedLinkListJson += "                        ],\n";
		expectedLinkListJson += "                        \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson += "                    }\n";
		expectedLinkListJson += "                ],\n";
		expectedLinkListJson += "                \"name\": \"collectedDataLinks\"\n";
		expectedLinkListJson += "            }],\n";
		expectedLinkListJson += "            \"name\": \"place:0001\"\n";
		expectedLinkListJson += "        }],\n";
		expectedLinkListJson += "        \"name\": \"place\"\n";
		expectedLinkListJson += "    }],\n";
		expectedLinkListJson += "    \"name\": \"linkLists\"\n";
		expectedLinkListJson += "}\n";
		Path path = Paths.get(basePath, "cora", LINK_LISTS_FILENAME);
		assertTrue(Files.exists(path));
		assertEquals(readJsonFileFromDisk(LINK_LISTS_FILENAME, "cora"), expectedLinkListJson);
	}

	@Test
	public void testRecordWithLinksOneRecordTypeWithoutLinks() throws IOException {
		DataGroup linkListWithTwoLinks = createLinkListWithTwoLinks("place:0001");
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, storageTerms, linkListWithTwoLinks,
				"cora");
		recordStorage.create("organisation", "organisation:0001", dataGroup, storageTerms,
				emptyLinkList, "cora");

		String expectedLinkListJson = "{\n";
		expectedLinkListJson += "    \"children\": [{\n";
		expectedLinkListJson += "        \"children\": [{\n";
		expectedLinkListJson += "            \"children\": [{\n";
		expectedLinkListJson += "                \"children\": [\n";
		expectedLinkListJson += "                    {\n";
		expectedLinkListJson += "                        \"children\": [\n";
		expectedLinkListJson += "                            {\n";
		expectedLinkListJson += "                                \"children\": [\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                        \"value\": \"fromRecordType\"\n";
		expectedLinkListJson += "                                    },\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                        \"value\": \"place:0001\"\n";
		expectedLinkListJson += "                                    }\n";
		expectedLinkListJson += "                                ],\n";
		expectedLinkListJson += "                                \"name\": \"from\"\n";
		expectedLinkListJson += "                            },\n";
		expectedLinkListJson += "                            {\n";
		expectedLinkListJson += "                                \"children\": [\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                        \"value\": \"toRecordType\"\n";
		expectedLinkListJson += "                                    },\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                        \"value\": \"toRecordId\"\n";
		expectedLinkListJson += "                                    }\n";
		expectedLinkListJson += "                                ],\n";
		expectedLinkListJson += "                                \"name\": \"to\"\n";
		expectedLinkListJson += "                            }\n";
		expectedLinkListJson += "                        ],\n";
		expectedLinkListJson += "                        \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson += "                    },\n";
		expectedLinkListJson += "                    {\n";
		expectedLinkListJson += "                        \"children\": [\n";
		expectedLinkListJson += "                            {\n";
		expectedLinkListJson += "                                \"children\": [\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                        \"value\": \"fromRecordType\"\n";
		expectedLinkListJson += "                                    },\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                        \"value\": \"place:0001\"\n";
		expectedLinkListJson += "                                    }\n";
		expectedLinkListJson += "                                ],\n";
		expectedLinkListJson += "                                \"name\": \"from\"\n";
		expectedLinkListJson += "                            },\n";
		expectedLinkListJson += "                            {\n";
		expectedLinkListJson += "                                \"children\": [\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                        \"value\": \"toRecordType\"\n";
		expectedLinkListJson += "                                    },\n";
		expectedLinkListJson += "                                    {\n";
		expectedLinkListJson += "                                        \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                        \"value\": \"toRecordId2\"\n";
		expectedLinkListJson += "                                    }\n";
		expectedLinkListJson += "                                ],\n";
		expectedLinkListJson += "                                \"name\": \"to\"\n";
		expectedLinkListJson += "                            }\n";
		expectedLinkListJson += "                        ],\n";
		expectedLinkListJson += "                        \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson += "                    }\n";
		expectedLinkListJson += "                ],\n";
		expectedLinkListJson += "                \"name\": \"collectedDataLinks\"\n";
		expectedLinkListJson += "            }],\n";
		expectedLinkListJson += "            \"name\": \"place:0001\"\n";
		expectedLinkListJson += "        }],\n";
		expectedLinkListJson += "        \"name\": \"place\"\n";
		expectedLinkListJson += "    }],\n";
		expectedLinkListJson += "    \"name\": \"linkLists\"\n";
		expectedLinkListJson += "}\n";
		Path path = Paths.get(basePath, "cora", LINK_LISTS_FILENAME);
		assertTrue(Files.exists(path));
		assertEquals(readJsonFileFromDisk(LINK_LISTS_FILENAME, "cora"), expectedLinkListJson);

	}

	private DataGroup createLinkListWithTwoLinks(String fromRecordId) {
		DataGroup linkList = DataCreator.createEmptyLinkList();

		linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
				TO_RECORD_TYPE, TO_RECORD_ID));

		linkList.addChild(DataCreator.createRecordToRecordLink(FROM_RECORD_TYPE, fromRecordId,
				TO_RECORD_TYPE, "toRecordId2"));
		return linkList;
	}

	@Test
	public void testRecordWithLinksTwoSystems() throws IOException {
		DataGroup linkListWithTwoLinks = createLinkListWithTwoLinks("place:0001");
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, storageTerms, linkListWithTwoLinks,
				"cora");
		recordStorage.create("place", "place:0002", dataGroup, storageTerms, linkListWithTwoLinks,
				"jsClient");
		recordStorage.create("place", "place:0003", dataGroup, storageTerms, linkListWithTwoLinks,
				"jsClient");
		recordStorage.create("organisation", "org:0001", dataGroup, storageTerms,
				linkListWithTwoLinks, "cora");
		recordStorage.create("organisation", "org:0002", dataGroup, storageTerms,
				linkListWithTwoLinks, "jsClient");

		Path path = Paths.get(basePath, "cora", LINK_LISTS_FILENAME);
		assertTrue(Files.exists(path));
		String expectedLinkListJson = "{\n";
		expectedLinkListJson += "    \"children\": [\n";
		expectedLinkListJson += "        {\n";
		expectedLinkListJson += "            \"children\": [{\n";
		expectedLinkListJson += "                \"children\": [{\n";
		expectedLinkListJson += "                    \"children\": [\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"children\": [\n";
		expectedLinkListJson += "                                {\n";
		expectedLinkListJson += "                                    \"children\": [\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                            \"value\": \"fromRecordType\"\n";
		expectedLinkListJson += "                                        },\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                            \"value\": \"place:0001\"\n";
		expectedLinkListJson += "                                        }\n";
		expectedLinkListJson += "                                    ],\n";
		expectedLinkListJson += "                                    \"name\": \"from\"\n";
		expectedLinkListJson += "                                },\n";
		expectedLinkListJson += "                                {\n";
		expectedLinkListJson += "                                    \"children\": [\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                            \"value\": \"toRecordType\"\n";
		expectedLinkListJson += "                                        },\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                            \"value\": \"toRecordId\"\n";
		expectedLinkListJson += "                                        }\n";
		expectedLinkListJson += "                                    ],\n";
		expectedLinkListJson += "                                    \"name\": \"to\"\n";
		expectedLinkListJson += "                                }\n";
		expectedLinkListJson += "                            ],\n";
		expectedLinkListJson += "                            \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson += "                        },\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"children\": [\n";
		expectedLinkListJson += "                                {\n";
		expectedLinkListJson += "                                    \"children\": [\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                            \"value\": \"fromRecordType\"\n";
		expectedLinkListJson += "                                        },\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                            \"value\": \"place:0001\"\n";
		expectedLinkListJson += "                                        }\n";
		expectedLinkListJson += "                                    ],\n";
		expectedLinkListJson += "                                    \"name\": \"from\"\n";
		expectedLinkListJson += "                                },\n";
		expectedLinkListJson += "                                {\n";
		expectedLinkListJson += "                                    \"children\": [\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                            \"value\": \"toRecordType\"\n";
		expectedLinkListJson += "                                        },\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                            \"value\": \"toRecordId2\"\n";
		expectedLinkListJson += "                                        }\n";
		expectedLinkListJson += "                                    ],\n";
		expectedLinkListJson += "                                    \"name\": \"to\"\n";
		expectedLinkListJson += "                                }\n";
		expectedLinkListJson += "                            ],\n";
		expectedLinkListJson += "                            \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson += "                        }\n";
		expectedLinkListJson += "                    ],\n";
		expectedLinkListJson += "                    \"name\": \"collectedDataLinks\"\n";
		expectedLinkListJson += "                }],\n";
		expectedLinkListJson += "                \"name\": \"org:0001\"\n";
		expectedLinkListJson += "            }],\n";
		expectedLinkListJson += "            \"name\": \"organisation\"\n";
		expectedLinkListJson += "        },\n";
		expectedLinkListJson += "        {\n";
		expectedLinkListJson += "            \"children\": [{\n";
		expectedLinkListJson += "                \"children\": [{\n";
		expectedLinkListJson += "                    \"children\": [\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"children\": [\n";
		expectedLinkListJson += "                                {\n";
		expectedLinkListJson += "                                    \"children\": [\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                            \"value\": \"fromRecordType\"\n";
		expectedLinkListJson += "                                        },\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                            \"value\": \"place:0001\"\n";
		expectedLinkListJson += "                                        }\n";
		expectedLinkListJson += "                                    ],\n";
		expectedLinkListJson += "                                    \"name\": \"from\"\n";
		expectedLinkListJson += "                                },\n";
		expectedLinkListJson += "                                {\n";
		expectedLinkListJson += "                                    \"children\": [\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                            \"value\": \"toRecordType\"\n";
		expectedLinkListJson += "                                        },\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                            \"value\": \"toRecordId\"\n";
		expectedLinkListJson += "                                        }\n";
		expectedLinkListJson += "                                    ],\n";
		expectedLinkListJson += "                                    \"name\": \"to\"\n";
		expectedLinkListJson += "                                }\n";
		expectedLinkListJson += "                            ],\n";
		expectedLinkListJson += "                            \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson += "                        },\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"children\": [\n";
		expectedLinkListJson += "                                {\n";
		expectedLinkListJson += "                                    \"children\": [\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                            \"value\": \"fromRecordType\"\n";
		expectedLinkListJson += "                                        },\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                            \"value\": \"place:0001\"\n";
		expectedLinkListJson += "                                        }\n";
		expectedLinkListJson += "                                    ],\n";
		expectedLinkListJson += "                                    \"name\": \"from\"\n";
		expectedLinkListJson += "                                },\n";
		expectedLinkListJson += "                                {\n";
		expectedLinkListJson += "                                    \"children\": [\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                                            \"value\": \"toRecordType\"\n";
		expectedLinkListJson += "                                        },\n";
		expectedLinkListJson += "                                        {\n";
		expectedLinkListJson += "                                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                                            \"value\": \"toRecordId2\"\n";
		expectedLinkListJson += "                                        }\n";
		expectedLinkListJson += "                                    ],\n";
		expectedLinkListJson += "                                    \"name\": \"to\"\n";
		expectedLinkListJson += "                                }\n";
		expectedLinkListJson += "                            ],\n";
		expectedLinkListJson += "                            \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson += "                        }\n";
		expectedLinkListJson += "                    ],\n";
		expectedLinkListJson += "                    \"name\": \"collectedDataLinks\"\n";
		expectedLinkListJson += "                }],\n";
		expectedLinkListJson += "                \"name\": \"place:0001\"\n";
		expectedLinkListJson += "            }],\n";
		expectedLinkListJson += "            \"name\": \"place\"\n";
		expectedLinkListJson += "        }\n";
		expectedLinkListJson += "    ],\n";
		expectedLinkListJson += "    \"name\": \"linkLists\"\n";
		expectedLinkListJson += "}\n";
		assertEquals(readJsonFileFromDisk(LINK_LISTS_FILENAME, "cora"), expectedLinkListJson);

		Path path2 = Paths.get(basePath, "jsClient", "linkLists_jsClient.json.gz");
		assertTrue(Files.exists(path2));
		String expectedLinkListJson2 = "{\n";
		expectedLinkListJson2 += "    \"children\": [\n";
		expectedLinkListJson2 += "        {\n";
		expectedLinkListJson2 += "            \"children\": [{\n";
		expectedLinkListJson2 += "                \"children\": [{\n";
		expectedLinkListJson2 += "                    \"children\": [\n";
		expectedLinkListJson2 += "                        {\n";
		expectedLinkListJson2 += "                            \"children\": [\n";
		expectedLinkListJson2 += "                                {\n";
		expectedLinkListJson2 += "                                    \"children\": [\n";
		expectedLinkListJson2 += "                                        {\n";
		expectedLinkListJson2 += "                                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson2 += "                                            \"value\": \"fromRecordType\"\n";
		expectedLinkListJson2 += "                                        },\n";
		expectedLinkListJson2 += "                                        {\n";
		expectedLinkListJson2 += "                                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson2 += "                                            \"value\": \"place:0001\"\n";
		expectedLinkListJson2 += "                                        }\n";
		expectedLinkListJson2 += "                                    ],\n";
		expectedLinkListJson2 += "                                    \"name\": \"from\"\n";
		expectedLinkListJson2 += "                                },\n";
		expectedLinkListJson2 += "                                {\n";
		expectedLinkListJson2 += "                                    \"children\": [\n";
		expectedLinkListJson2 += "                                        {\n";
		expectedLinkListJson2 += "                                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson2 += "                                            \"value\": \"toRecordType\"\n";
		expectedLinkListJson2 += "                                        },\n";
		expectedLinkListJson2 += "                                        {\n";
		expectedLinkListJson2 += "                                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson2 += "                                            \"value\": \"toRecordId\"\n";
		expectedLinkListJson2 += "                                        }\n";
		expectedLinkListJson2 += "                                    ],\n";
		expectedLinkListJson2 += "                                    \"name\": \"to\"\n";
		expectedLinkListJson2 += "                                }\n";
		expectedLinkListJson2 += "                            ],\n";
		expectedLinkListJson2 += "                            \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson2 += "                        },\n";
		expectedLinkListJson2 += "                        {\n";
		expectedLinkListJson2 += "                            \"children\": [\n";
		expectedLinkListJson2 += "                                {\n";
		expectedLinkListJson2 += "                                    \"children\": [\n";
		expectedLinkListJson2 += "                                        {\n";
		expectedLinkListJson2 += "                                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson2 += "                                            \"value\": \"fromRecordType\"\n";
		expectedLinkListJson2 += "                                        },\n";
		expectedLinkListJson2 += "                                        {\n";
		expectedLinkListJson2 += "                                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson2 += "                                            \"value\": \"place:0001\"\n";
		expectedLinkListJson2 += "                                        }\n";
		expectedLinkListJson2 += "                                    ],\n";
		expectedLinkListJson2 += "                                    \"name\": \"from\"\n";
		expectedLinkListJson2 += "                                },\n";
		expectedLinkListJson2 += "                                {\n";
		expectedLinkListJson2 += "                                    \"children\": [\n";
		expectedLinkListJson2 += "                                        {\n";
		expectedLinkListJson2 += "                                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson2 += "                                            \"value\": \"toRecordType\"\n";
		expectedLinkListJson2 += "                                        },\n";
		expectedLinkListJson2 += "                                        {\n";
		expectedLinkListJson2 += "                                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson2 += "                                            \"value\": \"toRecordId2\"\n";
		expectedLinkListJson2 += "                                        }\n";
		expectedLinkListJson2 += "                                    ],\n";
		expectedLinkListJson2 += "                                    \"name\": \"to\"\n";
		expectedLinkListJson2 += "                                }\n";
		expectedLinkListJson2 += "                            ],\n";
		expectedLinkListJson2 += "                            \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson2 += "                        }\n";
		expectedLinkListJson2 += "                    ],\n";
		expectedLinkListJson2 += "                    \"name\": \"collectedDataLinks\"\n";
		expectedLinkListJson2 += "                }],\n";
		expectedLinkListJson2 += "                \"name\": \"org:0002\"\n";
		expectedLinkListJson2 += "            }],\n";
		expectedLinkListJson2 += "            \"name\": \"organisation\"\n";
		expectedLinkListJson2 += "        },\n";
		expectedLinkListJson2 += "        {\n";
		expectedLinkListJson2 += "            \"children\": [\n";
		expectedLinkListJson2 += "                {\n";
		expectedLinkListJson2 += "                    \"children\": [{\n";
		expectedLinkListJson2 += "                        \"children\": [\n";
		expectedLinkListJson2 += "                            {\n";
		expectedLinkListJson2 += "                                \"children\": [\n";
		expectedLinkListJson2 += "                                    {\n";
		expectedLinkListJson2 += "                                        \"children\": [\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"fromRecordType\"\n";
		expectedLinkListJson2 += "                                            },\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"place:0001\"\n";
		expectedLinkListJson2 += "                                            }\n";
		expectedLinkListJson2 += "                                        ],\n";
		expectedLinkListJson2 += "                                        \"name\": \"from\"\n";
		expectedLinkListJson2 += "                                    },\n";
		expectedLinkListJson2 += "                                    {\n";
		expectedLinkListJson2 += "                                        \"children\": [\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"toRecordType\"\n";
		expectedLinkListJson2 += "                                            },\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"toRecordId\"\n";
		expectedLinkListJson2 += "                                            }\n";
		expectedLinkListJson2 += "                                        ],\n";
		expectedLinkListJson2 += "                                        \"name\": \"to\"\n";
		expectedLinkListJson2 += "                                    }\n";
		expectedLinkListJson2 += "                                ],\n";
		expectedLinkListJson2 += "                                \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson2 += "                            },\n";
		expectedLinkListJson2 += "                            {\n";
		expectedLinkListJson2 += "                                \"children\": [\n";
		expectedLinkListJson2 += "                                    {\n";
		expectedLinkListJson2 += "                                        \"children\": [\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"fromRecordType\"\n";
		expectedLinkListJson2 += "                                            },\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"place:0001\"\n";
		expectedLinkListJson2 += "                                            }\n";
		expectedLinkListJson2 += "                                        ],\n";
		expectedLinkListJson2 += "                                        \"name\": \"from\"\n";
		expectedLinkListJson2 += "                                    },\n";
		expectedLinkListJson2 += "                                    {\n";
		expectedLinkListJson2 += "                                        \"children\": [\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"toRecordType\"\n";
		expectedLinkListJson2 += "                                            },\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"toRecordId2\"\n";
		expectedLinkListJson2 += "                                            }\n";
		expectedLinkListJson2 += "                                        ],\n";
		expectedLinkListJson2 += "                                        \"name\": \"to\"\n";
		expectedLinkListJson2 += "                                    }\n";
		expectedLinkListJson2 += "                                ],\n";
		expectedLinkListJson2 += "                                \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson2 += "                            }\n";
		expectedLinkListJson2 += "                        ],\n";
		expectedLinkListJson2 += "                        \"name\": \"collectedDataLinks\"\n";
		expectedLinkListJson2 += "                    }],\n";
		expectedLinkListJson2 += "                    \"name\": \"place:0002\"\n";
		expectedLinkListJson2 += "                },\n";
		expectedLinkListJson2 += "                {\n";
		expectedLinkListJson2 += "                    \"children\": [{\n";
		expectedLinkListJson2 += "                        \"children\": [\n";
		expectedLinkListJson2 += "                            {\n";
		expectedLinkListJson2 += "                                \"children\": [\n";
		expectedLinkListJson2 += "                                    {\n";
		expectedLinkListJson2 += "                                        \"children\": [\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"fromRecordType\"\n";
		expectedLinkListJson2 += "                                            },\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"place:0001\"\n";
		expectedLinkListJson2 += "                                            }\n";
		expectedLinkListJson2 += "                                        ],\n";
		expectedLinkListJson2 += "                                        \"name\": \"from\"\n";
		expectedLinkListJson2 += "                                    },\n";
		expectedLinkListJson2 += "                                    {\n";
		expectedLinkListJson2 += "                                        \"children\": [\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"toRecordType\"\n";
		expectedLinkListJson2 += "                                            },\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"toRecordId\"\n";
		expectedLinkListJson2 += "                                            }\n";
		expectedLinkListJson2 += "                                        ],\n";
		expectedLinkListJson2 += "                                        \"name\": \"to\"\n";
		expectedLinkListJson2 += "                                    }\n";
		expectedLinkListJson2 += "                                ],\n";
		expectedLinkListJson2 += "                                \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson2 += "                            },\n";
		expectedLinkListJson2 += "                            {\n";
		expectedLinkListJson2 += "                                \"children\": [\n";
		expectedLinkListJson2 += "                                    {\n";
		expectedLinkListJson2 += "                                        \"children\": [\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"fromRecordType\"\n";
		expectedLinkListJson2 += "                                            },\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"place:0001\"\n";
		expectedLinkListJson2 += "                                            }\n";
		expectedLinkListJson2 += "                                        ],\n";
		expectedLinkListJson2 += "                                        \"name\": \"from\"\n";
		expectedLinkListJson2 += "                                    },\n";
		expectedLinkListJson2 += "                                    {\n";
		expectedLinkListJson2 += "                                        \"children\": [\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"toRecordType\"\n";
		expectedLinkListJson2 += "                                            },\n";
		expectedLinkListJson2 += "                                            {\n";
		expectedLinkListJson2 += "                                                \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson2 += "                                                \"value\": \"toRecordId2\"\n";
		expectedLinkListJson2 += "                                            }\n";
		expectedLinkListJson2 += "                                        ],\n";
		expectedLinkListJson2 += "                                        \"name\": \"to\"\n";
		expectedLinkListJson2 += "                                    }\n";
		expectedLinkListJson2 += "                                ],\n";
		expectedLinkListJson2 += "                                \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson2 += "                            }\n";
		expectedLinkListJson2 += "                        ],\n";
		expectedLinkListJson2 += "                        \"name\": \"collectedDataLinks\"\n";
		expectedLinkListJson2 += "                    }],\n";
		expectedLinkListJson2 += "                    \"name\": \"place:0003\"\n";
		expectedLinkListJson2 += "                }\n";
		expectedLinkListJson2 += "            ],\n";
		expectedLinkListJson2 += "            \"name\": \"place\"\n";
		expectedLinkListJson2 += "        }\n";
		expectedLinkListJson2 += "    ],\n";
		expectedLinkListJson2 += "    \"name\": \"linkLists\"\n";
		expectedLinkListJson2 += "}\n";
		assertEquals(readJsonFileFromDisk("linkLists_jsClient.json.gz", "jsClient"),
				expectedLinkListJson2);

		recordStorage.update("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");
		recordStorage.update("place", "place:0002", dataGroup, storageTerms, emptyLinkList,
				"jsClient");
		recordStorage.update("place", "place:0003", dataGroup, storageTerms, emptyLinkList,
				"jsClient");
		recordStorage.update("organisation", "org:0001", dataGroup, storageTerms, emptyLinkList,
				"cora");
		recordStorage.update("organisation", "org:0002", dataGroup, storageTerms, emptyLinkList,
				"jsClient");

		assertFalse(Files.exists(path));
		assertFalse(Files.exists(path2));
	}

	@Test
	public void testWriteCollectedDataToDiskOneRecordOneTerm() throws IOException {
		createRecordTypePlace();
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		List<StorageTerm> storageTerms = new ArrayList<>();
		storageTerms.add(new StorageTerm("placeNameStorageTerm", "Uppsala", "placeName"));

		recordStorage.create("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");

		String expectedCollectedDataOneTerm = "{\n";
		expectedCollectedDataOneTerm += "    \"children\": [{\n";
		expectedCollectedDataOneTerm += "        \"repeatId\": \"0\",\n";
		expectedCollectedDataOneTerm += "        \"children\": [\n";
		expectedCollectedDataOneTerm += "            {\n";
		expectedCollectedDataOneTerm += "                \"name\": \"type\",\n";
		expectedCollectedDataOneTerm += "                \"value\": \"place\"\n";
		expectedCollectedDataOneTerm += "            },\n";
		expectedCollectedDataOneTerm += "            {\n";
		expectedCollectedDataOneTerm += "                \"name\": \"key\",\n";
		expectedCollectedDataOneTerm += "                \"value\": \"placeName\"\n";
		expectedCollectedDataOneTerm += "            },\n";
		expectedCollectedDataOneTerm += "            {\n";
		expectedCollectedDataOneTerm += "                \"name\": \"id\",\n";
		expectedCollectedDataOneTerm += "                \"value\": \"place:0001\"\n";
		expectedCollectedDataOneTerm += "            },\n";
		expectedCollectedDataOneTerm += "            {\n";
		expectedCollectedDataOneTerm += "                \"name\": \"value\",\n";
		expectedCollectedDataOneTerm += "                \"value\": \"Uppsala\"\n";
		expectedCollectedDataOneTerm += "            },\n";
		expectedCollectedDataOneTerm += "            {\n";
		expectedCollectedDataOneTerm += "                \"name\": \"dataDivider\",\n";
		expectedCollectedDataOneTerm += "                \"value\": \"cora\"\n";
		expectedCollectedDataOneTerm += "            }\n";
		expectedCollectedDataOneTerm += "        ],\n";
		expectedCollectedDataOneTerm += "        \"name\": \"storageTerm\"\n";
		expectedCollectedDataOneTerm += "    }],\n";
		expectedCollectedDataOneTerm += "    \"name\": \"collectedData\"\n";
		expectedCollectedDataOneTerm += "}\n";

		Path path = Paths.get(basePath, "cora", COLLECTED_DATA_FILENAME);

		assertTrue(Files.exists(path));
		assertEquals(readJsonFileFromDisk(COLLECTED_DATA_FILENAME, "cora"),
				expectedCollectedDataOneTerm);
	}

	@Test
	public void testWriteCollectedDataToDiskTwoRecordOneTerm() throws IOException {
		createRecordTypePlace();
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		List<StorageTerm> storageTerms = new ArrayList<>();
		storageTerms.add(new StorageTerm("placeNameStorageTerm", "Uppsala", "placeName"));

		recordStorage.create("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");

		// DataGroup dataGroup2 = createDataGroupWithRecordInfo();
		DataGroup dataGroup2 = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("authority",
						"place", "place:0002");
		List<StorageTerm> storageTerms2 = new ArrayList<>();
		storageTerms2.add(new StorageTerm("placeNameStorageTerm", "Uppsala", "placeName"));

		recordStorage.create("place", "place:0002", dataGroup2, storageTerms2, emptyLinkList,
				"cora");

		String expectedCollectedDataOneTerm = "{\n";
		expectedCollectedDataOneTerm += "    \"children\": [\n";
		expectedCollectedDataOneTerm += "        {\n";
		expectedCollectedDataOneTerm += "            \"repeatId\": \"0\",\n";
		expectedCollectedDataOneTerm += "            \"children\": [\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"type\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"place\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"key\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"placeName\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"id\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"place:0001\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"value\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"Uppsala\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"dataDivider\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"cora\"\n";
		expectedCollectedDataOneTerm += "                }\n";
		expectedCollectedDataOneTerm += "            ],\n";
		expectedCollectedDataOneTerm += "            \"name\": \"storageTerm\"\n";
		expectedCollectedDataOneTerm += "        },\n";
		expectedCollectedDataOneTerm += "        {\n";
		expectedCollectedDataOneTerm += "            \"repeatId\": \"1\",\n";
		expectedCollectedDataOneTerm += "            \"children\": [\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"type\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"place\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"key\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"placeName\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"id\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"place:0002\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"value\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"Uppsala\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"dataDivider\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"cora\"\n";
		expectedCollectedDataOneTerm += "                }\n";
		expectedCollectedDataOneTerm += "            ],\n";
		expectedCollectedDataOneTerm += "            \"name\": \"storageTerm\"\n";
		expectedCollectedDataOneTerm += "        }\n";
		expectedCollectedDataOneTerm += "    ],\n";
		expectedCollectedDataOneTerm += "    \"name\": \"collectedData\"\n";
		expectedCollectedDataOneTerm += "}\n";

		Path path = Paths.get(basePath, "cora", COLLECTED_DATA_FILENAME);

		assertTrue(Files.exists(path));
		assertEquals(readJsonFileFromDisk(COLLECTED_DATA_FILENAME, "cora"),
				expectedCollectedDataOneTerm);
	}

	@Test
	public void testWriteCollectedDataToDiskOneRecordTwoTerms() throws IOException {
		createRecordTypePlace();
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();

		List<StorageTerm> storageTerms = new ArrayList<>();
		storageTerms.add(new StorageTerm("placeNameStorageTerm", "Uppsala", "placeName"));
		storageTerms.add(new StorageTerm("placeNameStorageTerm", "Stockholm", "placeName"));

		recordStorage.create("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");

		String expectedCollectedDataOneTerm = "{\n";
		expectedCollectedDataOneTerm += "    \"children\": [\n";
		expectedCollectedDataOneTerm += "        {\n";
		expectedCollectedDataOneTerm += "            \"repeatId\": \"0\",\n";
		expectedCollectedDataOneTerm += "            \"children\": [\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"type\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"place\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"key\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"placeName\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"id\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"place:0001\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"value\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"Uppsala\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"dataDivider\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"cora\"\n";
		expectedCollectedDataOneTerm += "                }\n";
		expectedCollectedDataOneTerm += "            ],\n";
		expectedCollectedDataOneTerm += "            \"name\": \"storageTerm\"\n";
		expectedCollectedDataOneTerm += "        },\n";
		expectedCollectedDataOneTerm += "        {\n";
		expectedCollectedDataOneTerm += "            \"repeatId\": \"1\",\n";
		expectedCollectedDataOneTerm += "            \"children\": [\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"type\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"place\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"key\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"placeName\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"id\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"place:0001\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"value\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"Stockholm\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"dataDivider\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"cora\"\n";
		expectedCollectedDataOneTerm += "                }\n";
		expectedCollectedDataOneTerm += "            ],\n";
		expectedCollectedDataOneTerm += "            \"name\": \"storageTerm\"\n";
		expectedCollectedDataOneTerm += "        }\n";
		expectedCollectedDataOneTerm += "    ],\n";
		expectedCollectedDataOneTerm += "    \"name\": \"collectedData\"\n";
		expectedCollectedDataOneTerm += "}\n";

		Path path = Paths.get(basePath, "cora", COLLECTED_DATA_FILENAME);

		assertTrue(Files.exists(path));
		assertEquals(readJsonFileFromDisk(COLLECTED_DATA_FILENAME, "cora"),
				expectedCollectedDataOneTerm);
	}

	@Test
	public void testWriteCollectedDataToDiskTwoRecordOneTermDifferentDataDividers()
			throws IOException {
		createRecordTypePlace();
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();

		List<StorageTerm> storageTerms = new ArrayList<>();
		storageTerms.add(new StorageTerm("placeNameStorageTerm", "Uppsala", "placeName"));

		recordStorage.create("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");

		DataGroup dataGroup2 = createDataGroupWithRecordInfo();
		DataGroup collectedData2 = DataCreator.createCollectedDataWithTypeAndId("place",
				"place:0002");
		DataGroup collectStorageTerm2 = new DataGroupSpy("storage");
		collectedData2.addChild(collectStorageTerm2);

		List<StorageTerm> storageTerms2 = new ArrayList<>();
		storageTerms2.add(new StorageTerm("placeNameStorageTerm", "Uppsala", "placeName"));

		recordStorage.create("place", "place:0002", dataGroup2, storageTerms2, emptyLinkList,
				"testSystem");

		String expectedCollectedDataOneTerm = "{\n";
		expectedCollectedDataOneTerm += "    \"children\": [{\n";
		expectedCollectedDataOneTerm += "        \"repeatId\": \"0\",\n";
		expectedCollectedDataOneTerm += "        \"children\": [\n";
		expectedCollectedDataOneTerm += "            {\n";
		expectedCollectedDataOneTerm += "                \"name\": \"type\",\n";
		expectedCollectedDataOneTerm += "                \"value\": \"place\"\n";
		expectedCollectedDataOneTerm += "            },\n";
		expectedCollectedDataOneTerm += "            {\n";
		expectedCollectedDataOneTerm += "                \"name\": \"key\",\n";
		expectedCollectedDataOneTerm += "                \"value\": \"placeName\"\n";
		expectedCollectedDataOneTerm += "            },\n";
		expectedCollectedDataOneTerm += "            {\n";
		expectedCollectedDataOneTerm += "                \"name\": \"id\",\n";
		expectedCollectedDataOneTerm += "                \"value\": \"place:0001\"\n";
		expectedCollectedDataOneTerm += "            },\n";
		expectedCollectedDataOneTerm += "            {\n";
		expectedCollectedDataOneTerm += "                \"name\": \"value\",\n";
		expectedCollectedDataOneTerm += "                \"value\": \"Uppsala\"\n";
		expectedCollectedDataOneTerm += "            },\n";
		expectedCollectedDataOneTerm += "            {\n";
		expectedCollectedDataOneTerm += "                \"name\": \"dataDivider\",\n";
		expectedCollectedDataOneTerm += "                \"value\": \"cora\"\n";
		expectedCollectedDataOneTerm += "            }\n";
		expectedCollectedDataOneTerm += "        ],\n";
		expectedCollectedDataOneTerm += "        \"name\": \"storageTerm\"\n";
		expectedCollectedDataOneTerm += "    }],\n";
		expectedCollectedDataOneTerm += "    \"name\": \"collectedData\"\n";
		expectedCollectedDataOneTerm += "}\n";

		Path path = Paths.get(basePath, "cora", COLLECTED_DATA_FILENAME);

		assertTrue(Files.exists(path));
		assertEquals(readJsonFileFromDisk(COLLECTED_DATA_FILENAME, "cora"),
				expectedCollectedDataOneTerm);

		String expectedCollectedDataOneTerm2 = "{\n";
		expectedCollectedDataOneTerm2 += "    \"children\": [{\n";
		expectedCollectedDataOneTerm2 += "        \"repeatId\": \"1\",\n";
		expectedCollectedDataOneTerm2 += "        \"children\": [\n";
		expectedCollectedDataOneTerm2 += "            {\n";
		expectedCollectedDataOneTerm2 += "                \"name\": \"type\",\n";
		expectedCollectedDataOneTerm2 += "                \"value\": \"place\"\n";
		expectedCollectedDataOneTerm2 += "            },\n";
		expectedCollectedDataOneTerm2 += "            {\n";
		expectedCollectedDataOneTerm2 += "                \"name\": \"key\",\n";
		expectedCollectedDataOneTerm2 += "                \"value\": \"placeName\"\n";
		expectedCollectedDataOneTerm2 += "            },\n";
		expectedCollectedDataOneTerm2 += "            {\n";
		expectedCollectedDataOneTerm2 += "                \"name\": \"id\",\n";
		expectedCollectedDataOneTerm2 += "                \"value\": \"place:0002\"\n";
		expectedCollectedDataOneTerm2 += "            },\n";
		expectedCollectedDataOneTerm2 += "            {\n";
		expectedCollectedDataOneTerm2 += "                \"name\": \"value\",\n";
		expectedCollectedDataOneTerm2 += "                \"value\": \"Uppsala\"\n";
		expectedCollectedDataOneTerm2 += "            },\n";
		expectedCollectedDataOneTerm2 += "            {\n";
		expectedCollectedDataOneTerm2 += "                \"name\": \"dataDivider\",\n";
		expectedCollectedDataOneTerm2 += "                \"value\": \"testSystem\"\n";
		expectedCollectedDataOneTerm2 += "            }\n";
		expectedCollectedDataOneTerm2 += "        ],\n";
		expectedCollectedDataOneTerm2 += "        \"name\": \"storageTerm\"\n";
		expectedCollectedDataOneTerm2 += "    }],\n";
		expectedCollectedDataOneTerm2 += "    \"name\": \"collectedData\"\n";
		expectedCollectedDataOneTerm2 += "}\n";

		Path path2 = Paths.get(basePath, "testSystem", "collectedData_testSystem.json.gz");

		assertTrue(Files.exists(path2));
		assertEquals(readJsonFileFromDisk("collectedData_testSystem.json.gz", "testSystem"),
				expectedCollectedDataOneTerm2);
	}

	@Test
	public void testWriteUpdatedCollectedDataToDiskOneRecordOneTerms() throws IOException {
		createRecordTypePlace();
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();

		List<StorageTerm> storageTerms = new ArrayList<>();
		storageTerms.add(new StorageTerm("placeNameStorageTerm", "Uppsala", "placeName"));
		storageTerms.add(new StorageTerm("placeNameStorageTerm", "Stockholm", "placeName"));

		recordStorage.create("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");

		DataGroup read = recordStorage.read("place", "place:0001");
		// collectStorageTerm.removeFirstChildWithNameInData("collectedDataTerm");
		storageTerms.remove(0);
		recordStorage.update("place", "place:0001", read, storageTerms, emptyLinkList, "cora");

		String expectedCollectedDataOneTerm = "{\n";
		expectedCollectedDataOneTerm += "    \"children\": [{\n";
		expectedCollectedDataOneTerm += "        \"repeatId\": \"0\",\n";
		expectedCollectedDataOneTerm += "        \"children\": [\n";
		expectedCollectedDataOneTerm += "            {\n";
		expectedCollectedDataOneTerm += "                \"name\": \"type\",\n";
		expectedCollectedDataOneTerm += "                \"value\": \"place\"\n";
		expectedCollectedDataOneTerm += "            },\n";
		expectedCollectedDataOneTerm += "            {\n";
		expectedCollectedDataOneTerm += "                \"name\": \"key\",\n";
		expectedCollectedDataOneTerm += "                \"value\": \"placeName\"\n";
		expectedCollectedDataOneTerm += "            },\n";
		expectedCollectedDataOneTerm += "            {\n";
		expectedCollectedDataOneTerm += "                \"name\": \"id\",\n";
		expectedCollectedDataOneTerm += "                \"value\": \"place:0001\"\n";
		expectedCollectedDataOneTerm += "            },\n";
		expectedCollectedDataOneTerm += "            {\n";
		expectedCollectedDataOneTerm += "                \"name\": \"value\",\n";
		expectedCollectedDataOneTerm += "                \"value\": \"Stockholm\"\n";
		expectedCollectedDataOneTerm += "            },\n";
		expectedCollectedDataOneTerm += "            {\n";
		expectedCollectedDataOneTerm += "                \"name\": \"dataDivider\",\n";
		expectedCollectedDataOneTerm += "                \"value\": \"cora\"\n";
		expectedCollectedDataOneTerm += "            }\n";
		expectedCollectedDataOneTerm += "        ],\n";
		expectedCollectedDataOneTerm += "        \"name\": \"storageTerm\"\n";
		expectedCollectedDataOneTerm += "    }],\n";
		expectedCollectedDataOneTerm += "    \"name\": \"collectedData\"\n";
		expectedCollectedDataOneTerm += "}\n";

		Path path = Paths.get(basePath, "cora", COLLECTED_DATA_FILENAME);

		assertTrue(Files.exists(path));
		assertEquals(readJsonFileFromDisk(COLLECTED_DATA_FILENAME, "cora"),
				expectedCollectedDataOneTerm);
	}

	@Test
	public void testWriteUpdatedCollectedDataToDiskOneRecordNoTerms() throws IOException {
		createRecordTypePlace();
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();

		List<StorageTerm> storageTerms2 = new ArrayList<>();
		storageTerms2.add(new StorageTerm("placeNameStorageTerm", "Uppsala", "placeName"));
		storageTerms2.add(new StorageTerm("placeNameStorageTerm", "Stockholm", "placeName"));

		recordStorage.create("place", "place:0001", dataGroup, storageTerms2, emptyLinkList,
				"cora");

		Path path = Paths.get(basePath, "cora", COLLECTED_DATA_FILENAME);
		assertTrue(Files.exists(path));

		DataGroup read = recordStorage.read("place", "place:0001");
		recordStorage.update("place", "place:0001", read, storageTerms, emptyLinkList, "cora");

		assertFalse(Files.exists(path));

		recordStorage.update("place", "place:0001", read, storageTerms, emptyLinkList, "cora");
		assertFalse(Files.exists(path));
	}

	@Test
	public void testDeletedCollectedDataToDiskOneRecordTwoTerms() throws IOException {
		createRecordTypePlace();
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		List<StorageTerm> storageTerms = new ArrayList<>();
		storageTerms.add(new StorageTerm("placeNameStorageTerm", "Uppsala", "placeName"));
		storageTerms.add(new StorageTerm("placeNameStorageTerm", "Stockholm", "placeName"));

		recordStorage.create("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");

		Path path = Paths.get(basePath, "cora", COLLECTED_DATA_FILENAME);
		assertTrue(Files.exists(path));

		recordStorage.deleteByTypeAndId("place", "place:0001");

		assertFalse(Files.exists(path));
	}

	@Test
	public void testDeletedCollectedDataToDiskTwoRecordOneTermDifferentDataDividers()
			throws IOException {
		createRecordTypePlace();
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		List<StorageTerm> storageTerms = new ArrayList<>();
		storageTerms.add(new StorageTerm("placeNameStorageTerm", "Uppsala", "placeName"));

		recordStorage.create("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");

		DataGroup dataGroup2 = createDataGroupWithRecordInfo();
		List<StorageTerm> storageTerms2 = new ArrayList<>();
		storageTerms2.add(new StorageTerm("placeNameStorageTerm", "Uppsala", "placeName"));

		recordStorage.create("place", "place:0002", dataGroup2, storageTerms2, emptyLinkList,
				"testSystem");

		Path path = Paths.get(basePath, "cora", COLLECTED_DATA_FILENAME);
		assertTrue(Files.exists(path));

		Path path2 = Paths.get(basePath, "testSystem", "collectedData_testSystem.json.gz");
		assertTrue(Files.exists(path2));

		recordStorage.deleteByTypeAndId("place", "place:0001");
		assertFalse(Files.exists(path));
		assertTrue(Files.exists(path2));

		recordStorage.deleteByTypeAndId("place", "place:0002");
		assertFalse(Files.exists(path));
		assertFalse(Files.exists(path2));
	}

	@Test
	public void testDeletedCollectedDataToDiskTwoRecordOneTerm() throws IOException {
		createRecordTypePlace();
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		List<StorageTerm> storageTerms = new ArrayList<>();
		storageTerms.add(new StorageTerm("placeNameStorageTerm", "Uppsala", "placeName"));

		recordStorage.create("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");

		DataGroup dataGroup2 = createDataGroupWithRecordInfo();

		List<StorageTerm> storageTerms2 = new ArrayList<>();
		storageTerms2.add(new StorageTerm("placeNameStorageTerm", "Uppsala", "placeName"));

		recordStorage.create("place", "place:0002", dataGroup2, storageTerms2, emptyLinkList,
				"cora");

		Path path = Paths.get(basePath, "cora", COLLECTED_DATA_FILENAME);
		assertTrue(Files.exists(path));

		recordStorage.deleteByTypeAndId("place", "place:0001");
		assertTrue(Files.exists(path));

		recordStorage.deleteByTypeAndId("place", "place:0002");
		assertFalse(Files.exists(path));
	}

	@Test
	public void testInitWithFileOnDiskNoLinksOnDisk() throws IOException {
		createRecordTypePlace();
		writeZippedPlaceFileToDisk();
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		DataGroup dataGroup = createDataGroupWithRecordInfo();

		assertJsonEqualDataGroup(dataGroupOut, dataGroup);

		DataGroup linkListPlace = recordStorage.readLinkList("place", "place:0001");
		assertJsonEqualDataGroup(linkListPlace, emptyLinkList);
	}

	private void assertJsonEqualDataGroup(DataGroup dataGroupActual, DataGroup dataGroupExpected) {
		assertEquals(convertDataGroupToJsonString(dataGroupActual),
				convertDataGroupToJsonString(dataGroupExpected));
	}

	private void writeZippedPlaceFileToDisk() throws IOException {
		writeZippedFileToDisk(expectedRecordJsonOneRecordPlace1, "cora", PLACE_CORA_FILENAME);
	}

	private void writeFileToDisk(String json, String dataDivider, String fileName)
			throws IOException {
		possiblyCreateFolderForDataDivider(dataDivider);
		Path path = FileSystems.getDefault().getPath(basePath, dataDivider, fileName);
		BufferedWriter writer;
		writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8, StandardOpenOption.CREATE);
		writer.write(json, 0, json.length());
		writer.flush();
		writer.close();
	}

	private void writeZippedFileToDisk(String json, String dataDivider, String fileName)
			throws IOException {
		possiblyCreateFolderForDataDivider(dataDivider);
		Path path = FileSystems.getDefault().getPath(basePath, dataDivider, fileName + ".gz");
		Writer writer2 = null;
		OutputStream newOutputStream = Files.newOutputStream(path, StandardOpenOption.CREATE);
		writer2 = new OutputStreamWriter(new GZIPOutputStream(newOutputStream), "UTF-8");

		writer2.write(json, 0, json.length());
		writer2.flush();
		writer2.close();
	}

	private void possiblyCreateFolderForDataDivider(String dataDivider) {
		Path pathIncludingDataDivider = Paths.get(basePath, dataDivider);
		File newPath = pathIncludingDataDivider.toFile();
		if (!newPath.exists()) {
			newPath.mkdir();
		}
	}

	private void writeDirectoryToDisk(String dirName) {
		Path path = FileSystems.getDefault().getPath(basePath, dirName);
		try {
			Files.createDirectory(path);
		} catch (IOException e) {
		}
	}

	private String convertDataGroupToJsonString(DataGroup dataGroup) {
		DataGroupToJsonConverter dataToJsonConverter = convertDataGroupToJson(dataGroup);
		return dataToJsonConverter.toJson();
		// return "";
	}

	private DataGroupToJsonConverter convertDataGroupToJson(DataGroup dataGroup) {
		// se.uu.ub.cora.json.builder.JsonBuilderFactory jsonBuilderFactory = new
		// se.uu.ub.cora.json.builder.org.OrgJsonBuilderFactoryAdapter();
		BasicDataToJsonConverterFactoryCreator factoryCreator = new BasicDataToJsonConverterFactoryCreator();
		DataToJsonConverterProvider.setDataToJsonConverterFactoryCreator(factoryCreator);
		DataToJsonConverterFactory dataToJsonConverterFactory = DataToJsonConverterProvider
				.createImplementingFactory();
		return (DataGroupToJsonConverter) dataToJsonConverterFactory
				.factorUsingConvertible(dataGroup);
		// .createForDataElement(jsonBuilderFactory, dataGroup);
		// return DataGroupToJsonConverter.usingJsonFactoryForDataGroup(jsonBuilderFactory,
		// dataGroup);
	}

	@Test
	public void testInitPlaceAndPersonFileOnDisk() throws IOException {
		createRecordTypePlace();
		DataGroup personRecordType = DataCreator
				.createRecordTypeWithIdAndUserSuppliedIdAndAbstract("person", "true", "false");
		recordStorage.create("recordType", "person", personRecordType, storageTerms, emptyLinkList,
				"cora");
		writeZippedPlaceFileToDisk();
		writePersonFileToDisk();
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		assertEquals(dataGroupOut.getNameInData(), "authority");
		DataGroup dataGroupPersonOut = recordStorage.read("person", "person:0001");
		assertEquals(dataGroupPersonOut.getNameInData(), "authority");
	}

	private void writePersonFileToDisk() throws IOException {
		String json = "{\"children\":[{\"children\":[{\"children\":[{\"name\":\"type\""
				+ ",\"value\":\"person\"}" + ",{\"name\":\"id\",\"value\":\"person:0001\"}]"
				+ ",\"name\":\"recordInfo\"}],\"name\":\"authority\"}],\"name\":\"recordList\"}";

		String fileName = PERSON_FILENAME;
		writeZippedFileToDisk(json, "cora", fileName);
	}

	@Test
	public void testUpdate() throws IOException {
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");

		dataGroup.addChild(new DataAtomicSpy("someNameInData", "someValue"));
		recordStorage.update("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");

		String expectedJson = "{\n";
		expectedJson += "    \"children\": [{\n";
		expectedJson += "        \"children\": [\n";
		expectedJson += "            {\n";
		expectedJson += "                \"children\": [\n";
		expectedJson += "                    {\n";
		expectedJson += "                        \"name\": \"type\",\n";
		expectedJson += "                        \"value\": \"place\"\n";
		expectedJson += "                    },\n";
		expectedJson += "                    {\n";
		expectedJson += "                        \"name\": \"id\",\n";
		expectedJson += "                        \"value\": \"place:0001\"\n";
		expectedJson += "                    },\n";
		expectedJson += "                    {\n";
		expectedJson += "                        \"children\": [\n";
		expectedJson += "                            {\n";
		expectedJson += "                                \"name\": \"linkedRecordType\",\n";
		expectedJson += "                                \"value\": \"system\"\n";
		expectedJson += "                            },\n";
		expectedJson += "                            {\n";
		expectedJson += "                                \"name\": \"linkedRecordId\",\n";
		expectedJson += "                                \"value\": \"cora\"\n";
		expectedJson += "                            }\n";
		expectedJson += "                        ],\n";
		expectedJson += "                        \"name\": \"dataDivider\"\n";
		expectedJson += "                    }\n";
		expectedJson += "                ],\n";
		expectedJson += "                \"name\": \"recordInfo\"\n";
		expectedJson += "            },\n";
		expectedJson += "            {\n";
		expectedJson += "                \"name\": \"someNameInData\",\n";
		expectedJson += "                \"value\": \"someValue\"\n";
		expectedJson += "            }\n";
		expectedJson += "        ],\n";
		expectedJson += "        \"name\": \"authority\"\n";
		expectedJson += "    }],\n";
		expectedJson += "    \"name\": \"recordList\"\n";
		expectedJson += "}\n";
		assertEquals(readJsonFileFromDisk(PLACE_CORA_FILENAME, "cora"), expectedJson);
	}

	@Test
	public void testDelete() throws IOException {
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");
		Path path = Paths.get(basePath, "cora", PLACE_CORA_FILENAME);
		assertTrue(Files.exists(path));

		recordStorage.deleteByTypeAndId("place", "place:0001");
		assertFalse(Files.exists(path));
	}

	@Test
	public void testDeleteRemoveOneRecord() throws IOException {
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");
		String readJsonFileFromDisk = readJsonFileFromDisk(PLACE_CORA_FILENAME, "cora");
		assertEquals(readJsonFileFromDisk, expectedRecordJsonOneRecordPlace1);

		DataGroup dataGroup2 = DataCreator
				.createDataGroupWithNameInDataAndRecordInfoWithRecordTypeAndRecordId("authority",
						"place", "place:0002");
		recordStorage.create("place", "place:0002", dataGroup2, storageTerms, emptyLinkList,
				"cora");

		assertEquals(readJsonFileFromDisk(PLACE_CORA_FILENAME, "cora"),
				expectedRecordJsonTwoRecords);

		recordStorage.deleteByTypeAndId("place", "place:0002");
		assertEquals(readJsonFileFromDisk(PLACE_CORA_FILENAME, "cora"),
				expectedRecordJsonOneRecordPlace1);
	}

	@Test
	public void testDeleteFileOnDiskRemovedWhenNoRecordsLeft() throws IOException {
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");
		Path path = Paths.get(basePath, "cora", PLACE_CORA_FILENAME);
		assertTrue(Files.exists(path));

		recordStorage.deleteByTypeAndId("place", "place:0001");
		assertFalse(Files.exists(path));
	}

	@Test
	public void testDeleteUnzippedFileOnDiskRemovedWhenNoRecordsLeft() throws IOException {
		createRecordTypePlace();
		writeFileToDisk(expectedRecordJsonOneRecordPlace1, "cora", "place_cora.json");

		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		Path path = Paths.get(basePath, "cora", "place_cora.json");
		assertTrue(Files.exists(path));

		recordStorage.deleteByTypeAndId("place", "place:0001");
		assertFalse(Files.exists(path));
	}

	@Test
	public void testUnzippedFileOnDiskRemovedWhenRecordUpdated() throws IOException {
		createRecordTypePlace();
		writeFileToDisk(expectedRecordJsonOneRecordPlace1, "cora", "place_cora.json");

		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		Path path = Paths.get(basePath, "cora", "place_cora.json");
		assertTrue(Files.exists(path));

		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.update("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");
		assertFalse(Files.exists(path));
	}

	@Test
	public void testUnzippedLinkListFileOnDiskRemovedWhenNoLinksLeft() throws IOException {
		createRecordTypePlace();
		writeFileToDisk(expectedRecordJsonOneRecordPlace1, "cora", "place_cora.json");
		writePlaceLinksFileToDisk();

		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		Path path = Paths.get(basePath, "cora", "linkLists_cora.json");
		assertTrue(Files.exists(path));

		recordStorage.deleteByTypeAndId("place", "place:0001");
		assertFalse(Files.exists(path));
	}

	@Test
	public void testUnzippedCollectedDataFileOnDiskRemovedWhenNoCollectedDataLeft()
			throws IOException {
		createRecordTypePlace();
		writeFileToDisk(expectedRecordJsonOneRecordPlace1, "cora", "place_cora.json");
		writeStorageTermsPlaceFileToDisk();

		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		Path path = Paths.get(basePath, "cora", "collectedData_cora.json");
		assertTrue(Files.exists(path));

		recordStorage.deleteByTypeAndId("place", "place:0001");
		assertFalse(Files.exists(path));
	}

	@Test
	public void testInitWithFileOnDiskLinksOnDisk() throws IOException {
		createRecordTypePlace();
		writeZippedPlaceFileToDisk();
		writeZippedPlaceLinksFileToDisk();

		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup dataGroupOut = recordStorage.read("place", "place:0001");
		DataGroup dataGroup = createDataGroupWithRecordInfo();

		assertJsonEqualDataGroup(dataGroupOut, dataGroup);

		DataGroup linkListPlace = recordStorage.readLinkList("place", "place:0001");
		String expectedLinkListJson = "{\n";
		expectedLinkListJson += "    \"children\": [\n";
		expectedLinkListJson += "        {\n";
		expectedLinkListJson += "            \"children\": [\n";
		expectedLinkListJson += "                {\n";
		expectedLinkListJson += "                    \"children\": [\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                            \"value\": \"fromRecordType\"\n";
		expectedLinkListJson += "                        },\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                            \"value\": \"place:0001\"\n";
		expectedLinkListJson += "                        }\n";
		expectedLinkListJson += "                    ],\n";
		expectedLinkListJson += "                    \"name\": \"from\"\n";
		expectedLinkListJson += "                },\n";
		expectedLinkListJson += "                {\n";
		expectedLinkListJson += "                    \"children\": [\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                            \"value\": \"toRecordType\"\n";
		expectedLinkListJson += "                        },\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                            \"value\": \"toRecordId\"\n";
		expectedLinkListJson += "                        }\n";
		expectedLinkListJson += "                    ],\n";
		expectedLinkListJson += "                    \"name\": \"to\"\n";
		expectedLinkListJson += "                },\n";
		expectedLinkListJson += "                {\n";
		expectedLinkListJson += "                    \"children\": [\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                            \"value\": \"toRecordType\"\n";
		expectedLinkListJson += "                        },\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                            \"value\": \"toRecordId\"\n";
		expectedLinkListJson += "                        }\n";
		expectedLinkListJson += "                    ],\n";
		expectedLinkListJson += "                    \"name\": \"to\"\n";
		expectedLinkListJson += "                }\n";
		expectedLinkListJson += "            ],\n";
		expectedLinkListJson += "            \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson += "        },\n";
		expectedLinkListJson += "        {\n";
		expectedLinkListJson += "            \"children\": [\n";
		expectedLinkListJson += "                {\n";
		expectedLinkListJson += "                    \"children\": [\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                            \"value\": \"fromRecordType\"\n";
		expectedLinkListJson += "                        },\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                            \"value\": \"place:0001\"\n";
		expectedLinkListJson += "                        }\n";
		expectedLinkListJson += "                    ],\n";
		expectedLinkListJson += "                    \"name\": \"from\"\n";
		expectedLinkListJson += "                },\n";
		expectedLinkListJson += "                {\n";
		expectedLinkListJson += "                    \"children\": [\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                            \"value\": \"toRecordType\"\n";
		expectedLinkListJson += "                        },\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                            \"value\": \"toRecordId2\"\n";
		expectedLinkListJson += "                        }\n";
		expectedLinkListJson += "                    ],\n";
		expectedLinkListJson += "                    \"name\": \"to\"\n";
		expectedLinkListJson += "                },\n";
		expectedLinkListJson += "                {\n";
		expectedLinkListJson += "                    \"children\": [\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"name\": \"linkedRecordType\",\n";
		expectedLinkListJson += "                            \"value\": \"toRecordType\"\n";
		expectedLinkListJson += "                        },\n";
		expectedLinkListJson += "                        {\n";
		expectedLinkListJson += "                            \"name\": \"linkedRecordId\",\n";
		expectedLinkListJson += "                            \"value\": \"toRecordId2\"\n";
		expectedLinkListJson += "                        }\n";
		expectedLinkListJson += "                    ],\n";
		expectedLinkListJson += "                    \"name\": \"to\"\n";
		expectedLinkListJson += "                }\n";
		expectedLinkListJson += "            ],\n";
		expectedLinkListJson += "            \"name\": \"recordToRecordLink\"\n";
		expectedLinkListJson += "        }\n";
		expectedLinkListJson += "    ],\n";
		expectedLinkListJson += "    \"name\": \"collectedDataLinks\"\n";
		expectedLinkListJson += "}";
		assertEquals(convertDataGroupToJsonString(linkListPlace), expectedLinkListJson);

		DataGroup dataGroupTo = createDataGroupWithRecordInfo();
		recordStorage.create("toRecordType", "toRecordId", dataGroupTo, storageTerms, emptyLinkList,
				"cora");
		Collection<DataGroup> incomingLinksTo = recordStorage
				.generateLinkCollectionPointingToRecord("toRecordType", "toRecordId");

		assertEquals(incomingLinksTo.size(), 1);
	}

	@Test
	public void testListWithCollectedStorageTermReadWithMatchingUppsalaFilter() throws IOException {
		createRecordTypePlace();
		writeZippedPlaceFileToDisk();
		writeZippedStorageTermsPlaceFileToDisk();
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup filter = DataCreator.createEmptyFilter();
		DataGroup part = DataCreator.createFilterPartWithRepeatIdAndKeyAndValue("0", "placeName",
				"Uppsala");
		filter.addChild(part);

		Collection<DataGroup> readList = recordStorage.readList("place", filter).listOfDataGroups;
		assertEquals(readList.size(), 1);
		DataGroup first = readList.iterator().next();
		assertEquals(first.getFirstGroupWithNameInData("recordInfo")
				.getFirstAtomicValueWithNameInData("id"), "place:0001");
	}

	@Test(expectedExceptions = RecordNotFoundException.class)
	// @Test
	public void testReadingEmptyCollectedDataBeforeReadingRecordFiles() throws IOException {
		writeZippedStorageTermsPlaceFileToDisk();
		RecordStorageOnDisk recordStorage = RecordStorageOnDisk
				.createRecordStorageOnDiskWithBasePath(basePath);

		DataGroup filter = DataCreator.createEmptyFilter();
		DataGroup part = DataCreator.createFilterPartWithRepeatIdAndKeyAndValue("0", "placeName",
				"Uppsala");
		filter.addChild(part);

		recordStorage.readList("place", filter);
	}

	private void writeZippedPlaceLinksFileToDisk() throws IOException {
		String expectedLinkListJson = createJsonForPlaceLinks();
		writeZippedFileToDisk(expectedLinkListJson, "cora", LINK_LISTS_FILENAME);
	}

	private void writePlaceLinksFileToDisk() throws IOException {
		String expectedLinkListJson = createJsonForPlaceLinks();
		writeFileToDisk(expectedLinkListJson, "cora", "linkLists_cora.json");
	}

	private String createJsonForPlaceLinks() {
		String expectedLinkListJson = "{\"children\":[{\"children\":[{\"children\":[{\"children\":["
				+ "{\"children\":[{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"fromRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"place:0001\"}]"
				+ ",\"name\":\"from\"},{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"toRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"toRecordId\"}]"
				+ ",\"name\":\"to\"},{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"toRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"toRecordId\"}]" + ",\"name\":\"to\"}]"
				+ ",\"name\":\"recordToRecordLink\"}" + ",{\"children\":[{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"fromRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"place:0001\"}]"
				+ ",\"name\":\"from\"},{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"toRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"toRecordId2\"}]"
				+ ",\"name\":\"to\"},{\"children\":["
				+ "{\"name\":\"linkedRecordType\",\"value\":\"toRecordType\"}"
				+ ",{\"name\":\"linkedRecordId\",\"value\":\"toRecordId2\"}]"
				+ ",\"name\":\"to\"}],\"name\":\"recordToRecordLink\"}]"
				+ ",\"name\":\"collectedDataLinks\"}],\"name\":\"place:0001\"}]"
				+ ",\"name\":\"place\"}],\"name\":\"linkLists\"}";
		return expectedLinkListJson;
	}

	private void writeZippedStorageTermsPlaceFileToDisk() throws IOException {
		String expectedCollectedDataOneTerm = getStorageTermJson();
		writeZippedFileToDisk(expectedCollectedDataOneTerm, "cora", COLLECTED_DATA_FILENAME);
	}

	private void writeStorageTermsPlaceFileToDisk() throws IOException {
		String expectedCollectedDataOneTerm = getStorageTermJson();
		writeFileToDisk(expectedCollectedDataOneTerm, "cora", "collectedData_cora.json");
	}

	private String getStorageTermJson() {
		String expectedCollectedDataOneTerm = "{\n";
		expectedCollectedDataOneTerm += "    \"children\": [\n";
		expectedCollectedDataOneTerm += "        {\n";
		expectedCollectedDataOneTerm += "            \"repeatId\": \"0\",\n";
		expectedCollectedDataOneTerm += "            \"children\": [\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"type\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"place\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"key\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"placeName\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"id\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"place:0001\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"value\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"Uppsala\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"dataDivider\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"cora\"\n";
		expectedCollectedDataOneTerm += "                }\n";
		expectedCollectedDataOneTerm += "            ],\n";
		expectedCollectedDataOneTerm += "            \"name\": \"storageTerm\"\n";
		expectedCollectedDataOneTerm += "        },\n";
		expectedCollectedDataOneTerm += "        {\n";
		expectedCollectedDataOneTerm += "            \"repeatId\": \"1\",\n";
		expectedCollectedDataOneTerm += "            \"children\": [\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"type\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"place\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"key\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"placeName\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"id\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"place:0001\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"value\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"Stockholm\"\n";
		expectedCollectedDataOneTerm += "                },\n";
		expectedCollectedDataOneTerm += "                {\n";
		expectedCollectedDataOneTerm += "                    \"name\": \"dataDivider\",\n";
		expectedCollectedDataOneTerm += "                    \"value\": \"cora\"\n";
		expectedCollectedDataOneTerm += "                }\n";
		expectedCollectedDataOneTerm += "            ],\n";
		expectedCollectedDataOneTerm += "            \"name\": \"storageTerm\"\n";
		expectedCollectedDataOneTerm += "        }\n";
		expectedCollectedDataOneTerm += "    ],\n";
		expectedCollectedDataOneTerm += "    \"name\": \"collectedData\"\n";
		expectedCollectedDataOneTerm += "}\n";
		return expectedCollectedDataOneTerm;
	}

	@Test(expectedExceptions = DataStorageException.class)
	public void testInitMissingPath() throws IOException {
		removeTempFiles();
		RecordStorageOnDisk.createRecordStorageOnDiskWithBasePath(basePath);
	}

	@Test
	public void testInitMissingPathSendsAlongException() throws IOException {
		removeTempFiles();
		Exception caughtException = null;
		try {
			RecordStorageOnDisk.createRecordStorageOnDiskWithBasePath(basePath);
		} catch (Exception e) {
			caughtException = e;
		}
		assertTrue(caughtException.getCause() instanceof NoSuchFileException);
		assertEquals(caughtException.getMessage(), "can not read files from disk on init: "
				+ "java.nio.file.NoSuchFileException: /tmp/recordStorageOnDiskTemp");
	}

	@Test
	public void testCreateFileInLockedDir() throws IOException {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");
		Paths.get(basePath, "cora").toFile().setWritable(false);
		try {
			recordStorage.create("organisation", "org:0001", dataGroup, storageTerms, emptyLinkList,
					"cora");
		} catch (Exception e) {
			assertTrue(e.getCause() instanceof AccessDeniedException);
			assertEquals(e.getMessage(),
					"can not write files to disk: java.nio.file.AccessDeniedException: "
							+ "/tmp/recordStorageOnDiskTemp/cora/organisation_cora.json.gz");
		} finally {
			Paths.get(basePath, "cora").toFile().setWritable(true);
		}
	}

	@Test(expectedExceptions = DataStorageException.class, expectedExceptionsMessageRegExp = ""
			+ "Could not make directory /tmp/recordStorageOnDiskTemp/cora")
	public void testCreateDirWhenParentIsGone() throws IOException {
		removeTempFiles();
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");
	}

	@Test(expectedExceptions = DataStorageException.class)
	public void testUpdateMissingPath() throws IOException {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");
		removeTempFiles();
		recordStorage.update("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");

	}

	@Test(expectedExceptions = DataStorageException.class)
	public void testDeleteMissingPath() throws IOException {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");
		removeTempFiles();
		recordStorage.deleteByTypeAndId("place", "place:0001");
	}

	@Test
	public void testDeleteMissingPathSentAlongException() throws IOException {
		Exception caughtException = null;
		try {
			DataGroup dataGroup = createDataGroupWithRecordInfo();
			recordStorage.create("place", "place:0001", dataGroup, storageTerms, emptyLinkList,
					"cora");
			removeTempFiles();
			recordStorage.deleteByTypeAndId("place", "place:0001");
		} catch (Exception e) {
			caughtException = e;
		}
		assertTrue(caughtException.getCause() instanceof NoSuchFileException);
		assertEquals(caughtException.getMessage(),
				"can not delete record files from diskjava.nio.file.NoSuchFileException: "
						+ "/tmp/recordStorageOnDiskTemp/cora/place_cora.json.gz");
	}

	@Test(expectedExceptions = DataStorageException.class, expectedExceptionsMessageRegExp = ""
			+ "can not delete directory from disk/tmp/recordStorageOnDiskTemp/cora")
	public void testCanNotDeleteDataDividerFolder() throws IOException {
		DataGroup dataGroup = createDataGroupWithRecordInfo();
		recordStorage.create("place", "place:0001", dataGroup, storageTerms, emptyLinkList, "cora");
		File dir = Paths.get(basePath, "cora").toFile();
		recordStorage.deleteDirectory(dir);
	}

	@Test(expectedExceptions = DataStorageException.class, expectedExceptionsMessageRegExp = ""
			+ "Symbolic link points to missing path: /tmp/recordStorageOnDiskTemp/linkTest")
	public void testStartWithSymbolicLinkPointingToNothing() throws IOException {
		Path link = FileSystems.getDefault().getPath(basePath, "linkTest");
		Path target = FileSystems.getDefault().getPath(basePath, "nonExistingDir");

		Files.createSymbolicLink(link, target);
		RecordStorageOnDisk.createRecordStorageOnDiskWithBasePath(basePath);
	}
}
