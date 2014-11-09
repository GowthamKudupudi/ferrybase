/* 
 * File:   ffjsonTest.cpp
 * Author: Gowtham
 *
 * Created on Sep 22, 2014, 2:12:12 PM
 */

#include <stdlib.h>
#include <iostream>
#include "FFJSON.h"
#include "logger.h"
#include <string>
#include <fstream>
#include <streambuf>
#include <stdio.h>  /* defines FILENAME_MAX */
#ifdef WINDOWS
#include <direct.h>
#define GetCurrentDir _getcwd
#else
#include <unistd.h>
#include <list>
#define GetCurrentDir getcwd
#endif

/*
 * Simple C++ Test Suite
 */

using namespace std;

int ff_log_type = FFL_DEBUG | FFL_INFO;
unsigned int ff_log_level = 1;
int child_exit_status = 0;

void test1() {
	std::cout << "ffjsonTest test 1" << std::endl;
	char cCurrentPath[FILENAME_MAX];

	if (!GetCurrentDir(cCurrentPath, sizeof (cCurrentPath))) {
		return;
	}

	cCurrentPath[sizeof (cCurrentPath) - 1] = '\0'; /* not really required */
	char c = '\r';
	ffl_info(1, "\\r=%d", (int) c);
	ffl_info(1, "The current working directory is %s", cCurrentPath);
	fflush(stdout);
	string fn = "sample.ffjson";
	//string fn = "/home/gowtham/Projects/ferrymediaserver/output.ffjson";
	ifstream ifs(fn.c_str(), ios::in | ios::ate);
	string ffjsonStr;
	ifs.seekg(0, std::ios::end);
	ffjsonStr.reserve(ifs.tellg());
	ifs.seekg(0, std::ios::beg);
	ffjsonStr.assign((std::istreambuf_iterator<char>(ifs)),
			std::istreambuf_iterator<char>());
	FFJSON ffo(ffjsonStr);
	cout << "amphibians: ";
	FFJSON::Iterator i = ffo["amphibians"].begin(); //["amphibians"]
	while ((i != ffo["amphibians"].end())) {
		cout << string(i) << ":" << i->stringify() << endl;
		++i;
	}
	cout << endl;
	std::string ps = ffo.stringify();
	cout << ps << endl;
	FFJSON ffo2(ps);
	ffo2["amphibians"]["genome"].setEFlag(FFJSON::E_FLAGS::B64ENCODE);
	ffo2["amphibians"]["salamanders"] = "salee";
	std::string ps2 = ffo2.prettyString();
	cout << ps2 << endl;
	ffo2["amphibians"]["salamanders"] = "malee";
	ffo2["amphibians"]["count"] = 4;
	ffo2["amphibians"]["debnsity"] = 5.6;
	ffo2["amphibians"]["count"] = 5;
	ffo2["amphibians"]["genome"] = "<xml>sadfalejhjroifndk</xml>";
	if (ffo2["amphibians"]["gowtham"]) {
		cout << ffo2["amphibians"].size << endl;
		ffo2["amphibians"].trim();
		cout << ffo2["amphibians"].size << endl;
	};
	ffo2["animals"][3] = "satish";
	cout << ffo2["animals"][4].prettyString() << endl;
	cout << "size: " << ffo2["animals"].size << endl;
	ffo2["animals"][3] = "bear";
	cout << "after bear inserted at 4" << ffo2["animals"].prettyString() << endl;
	cout << "size: " << ffo2["animals"].size << endl;
	ffo2["animals"].trim();
	cout << "size after trim: " << ffo2["animals"].size << endl;
	std::string ps3 = ffo2.prettyString();
	cout << ps3 << endl;
	cout << "FFJSON signature size: " << sizeof (ffo2) << endl;

	ffo2["amphibians"]["frogs"].setQType(FFJSON::QUERY_TYPE::QUERY);
	ffo2["amphibians"]["salamanders"].setQType(FFJSON::QUERY_TYPE::DELETE);
	ffo2["amphibians"]["genome"].setQType(FFJSON::QUERY_TYPE::SET);
	ffo2["birds"][1].setQType(FFJSON::QUERY_TYPE::DELETE);
	ffo2["birds"][2].setQType(FFJSON::QUERY_TYPE::SET);
	ffo2["birds"][3].setQType(FFJSON::QUERY_TYPE::QUERY);
	string query = ffo2.queryString();
	ffo2["amphibians"]["genome"] = "<xml>gnomechanged :p</xml>";
	ffo2["birds"][2] = "kiwi";
	cout << ffo2.prettyString() << endl;
	cout << query << endl;
	FFJSON qo(query);
	query = qo.queryString();
	cout << query << endl;
	FFJSON* ao = ffo2.answerObject(&qo);
	cout << ao->stringify() << endl;
	string ffo2a = ffo2.prettyString();
	cout << ffo2a << endl;
	FFJSON ffo2ao(ffo2a);
	ffo2a = ffo2ao.stringify();
	cout << ffo2a << endl;
	ffo2a = ffo2ao.prettyString();
	cout << ffo2a << endl;
	delete ao;
	return;
}

struct testStruct {
	string* s;
};

void test2() {
	//std::cout << "ffjsonTest test 2" << std::endl;
	//string fn = "file:///home/gowtham/Projects/ferrymediaserver/offpmpack.json";
	string fn = "file://sample.ffjson";
	//	ifstream ifs(fn.c_str(), ios::in | ios::ate);
	//	string ffjsonStr;
	//	ifs.seekg(0, std::ios::end);
	//	ffjsonStr.reserve(ifs.tellg());
	//	ifs.seekg(0, std::ios::beg);
	//	ffjsonStr.assign((std::istreambuf_iterator<char>(ifs)),
	//			std::istreambuf_iterator<char>());
	FFJSON ffo(fn);
	ffo["ferryframes"].setEFlag(FFJSON::B64ENCODE);
	string* s = new string(ffo.stringify(true));
	cout << *s << endl;
	FFJSON ffo2(fn);
	cout << ffo2.prettyString() << endl;
	s->append(":)");
	testStruct ts;
	ts.s = s;
	delete ts.s;
	cout << "%TEST_PASSED%" << endl;
}

int main(int argc, char** argv) {
	std::cout << "%SUITE_STARTING% ffjsonTest" << std::endl;
	std::cout << "%SUITE_STARTED%" << std::endl;

	std::cout << "%TEST_STARTED% test1 (ffjsonTest)" << std::endl;
	test1();
	std::cout << "%TEST_FINISHED% time=0 test1 (ffjsonTest)" << std::endl;

	std::cout << "%TEST_STARTED% test2 (ffjsonTest)\n" << std::endl;
	test2();
	std::cout << "%TEST_FINISHED% time=0 test2 (ffjsonTest)" << std::endl;

	std::cout << "%SUITE_FINISHED% time=0" << std::endl;

	return (EXIT_SUCCESS);
}

