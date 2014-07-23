/* 
 * File:   FFJSON.cpp
 * Author: raheem
 * 
 * Created on November 29, 2013, 4:29 PM
 */

#include <string>
#include <malloc.h>
#include <math.h>
#include <string.h>
#include <openssl/bio.h>
#include <openssl/evp.h>

#include "FFJSON.h"
#include "mystdlib.h"
#include "myconverters.h"

FFJSON::FFJSON() {
}

FFJSON::FFJSON(std::string& ffjson) {
    trimWhites(ffjson);
    this->ffjson = ffjson;
    this->type = objectType(ffjson);
    if (this->type == FFJSON_OBJ_TYPE::OBJECT) {
        int i = 1;
        int l = ffjson.length();
        int j = 1;
        int elementStartPos = 0;
        std::string property;
        std::string value;
        int k = 0;
        bool propset = false;
        bool valset = false;
        bool array = false;
        bool object = false;
        bool ffescstr = false;
        bool success = false;
        bool str = false;
        int recursiveObjectCount = 0;
        int recursiveArrayCount = 0;
        this->val.pairs = new std::map<std::string, FFJSON*>();
        while (i < l) {
oloopbegining:
            if (!propset) {
                if (ffjson[i] == ':') {
                    property = ffjson.substr(j, i - j);
                    trimWhites(property);
                    trimQuotes(property);
                    propset = true;
                    j = i;
                } else if (ffjson[i] == ',' || ffjson[i] == '\\' || ffjson[i] == '.') {
                    int k = 0;
                    throw Exception("unexpected character '" + std::string(1, (char) ffjson[i]) + "' in property at " + std::string(itoa(i)) + " in ''..." + std::string((char*) (ffjson.c_str() + (i > 10 ? i - 10 : i)), ((k = (ffjson.length() - i)) >= 10 ? 20 : k)) + "...'");
                }
            } else {
                if (ffjson[i] == '\\' && !ffescstr && !str) {
                    i++;
                } else if (ffjson[i] == '{' && !array&& !ffescstr && !str) {
                    object = true;
                    recursiveObjectCount++;
                } else if (ffjson[i] == '}' && !array&& !ffescstr && object && !str) {
                    recursiveObjectCount--;
                    if (recursiveObjectCount == 0) {
                        object = false;
                    }
                } else if (ffjson[i] == '[' && !object && !ffescstr && !str) {
                    recursiveArrayCount++;
                    array = true;
                } else if (ffjson[i] == ']' && !object&& !ffescstr && array && !str) {
                    recursiveArrayCount--;
                    if (recursiveArrayCount == 0)
                        array = false;
                } else if ((ffjson[i] == ',' || ffjson[i] == '}')&&!array&&!object && !ffescstr && !str) {
                    FFJSON* f = new FFJSON(value);
                    propset = false;
                    j = i + 1;
                    (*this->val.pairs)[property] = f;
                    if (ffjson[i] == '}')success = true;
                    value = "";
                    valset = true;
                } else if (ffjson[i] == '"' && !ffescstr) {
                    str = !str;
                } else if (ffjson[i] == 'F' && !str) {
                    if (ffjson[i + 1] == 'F') {
                        if (ffjson[i + 2] == 'E') {
                            if (ffjson[i + 3] == 'S') {
                                if (ffjson[i + 4] == 'C') {
                                    if (ffjson[i + 5] == 'S') {
                                        if (ffjson[i + 6] == 'T') {
                                            if (ffjson[i + 7] == 'R') {
                                                ffescstr = !ffescstr;
                                                i += 8;
                                                value.append("FFESCSTR");
                                                goto oloopbegining;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                if (!valset) {
                    value += ffjson[i];
                } else {
                    valset = false;
                }
                k++;
            }
            i++;
        }
        if (!success) {
            throw Exception("Terminating character not found.");
        }
    } else if (this->type == FFJSON_OBJ_TYPE::ARRAY) {
        int i = 1;
        int l = ffjson.length();
        int j = 1;
        int elementStartPos = 0;
        std::string property;
        std::string value;
        int k = 0;
        bool propset = false;
        bool valset = false;
        bool array = false;
        bool object = false;
        bool ffescstr = false;
        bool str = false;
        int recursiveObjectCount = 0;
        int recursiveArrayCount = 0;
        int elementCount = 0;
        this->val.array = new std::vector<FFJSON*>();
        bool success = false;
        while (i < l) {
aloopbegining:
            if (ffjson[i] == '\\' && !ffescstr && !str) {
                i++;
            } else if (ffjson[i] == '{' && !array&& !ffescstr && !str) {
                object = true;
                recursiveObjectCount++;
            } else if (ffjson[i] == '}' && !array&& !ffescstr && object && !str) {
                recursiveObjectCount--;
                if (recursiveObjectCount == 0) {
                    object = false;
                }
            } else if (ffjson[i] == '[' && !object && !ffescstr && !str) {
                recursiveArrayCount++;
                array = true;
            } else if (ffjson[i] == ']' && !object&& !ffescstr && array && !str) {
                recursiveArrayCount--;
                if (recursiveArrayCount == 0)
                    array = false;
            } else if ((ffjson[i] == ',' || ffjson[i] == ']') && !array && !object && !ffescstr && !str) {
                FFJSON* f = new FFJSON(value);
                propset = false;
                j = i + 1;
                (*this->val.array).push_back(f);
                elementCount++;
                if (ffjson[i] == ']')success = true;
                value = "";
                valset = true;
            } else if (ffjson[i] == '"' && !ffescstr) {
                str = !str;
            } else if (ffjson[i] == 'F' && !str) {
                if (ffjson[i + 1] == 'F') {
                    if (ffjson[i + 2] == 'E') {
                        if (ffjson[i + 3] == 'S') {
                            if (ffjson[i + 4] == 'C') {
                                if (ffjson[i + 5] == 'S') {
                                    if (ffjson[i + 6] == 'T') {
                                        if (ffjson[i + 7] == 'R') {
                                            ffescstr = !ffescstr;
                                            i += 8;
                                            value.append("FFESCSTR");
                                            goto aloopbegining;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if (!valset) {
                value += ffjson[i];
                k++;
            } else {
                valset = false;
                k = 0;
            }
            i++;
        }
        if (!success) {
            throw Exception("Terminating character not found.");
        }
    } else if (this->type == FFJSON_OBJ_TYPE::STRING) {
        this->val.string = (char*) malloc(ffjson.length() - 1);
        memcpy(this->val.string, ffjson.c_str() + 1, ffjson.length() - 2);
        this->val.string[ffjson.length() - 2] = '\0';
        this->length = ffjson.length() - 2;
    } else if (this->type == FFJSON_OBJ_TYPE::BOOL) {
        this->val.boolean = ((int) ffjson.find("true", 0)) != -1 ? true : false;
    } else if (this->type == FFJSON_OBJ_TYPE::UNRECOGNIZED) {
        if (ffjson.length() > 20) {
            int startIndex = 0;
            int endIndex = ffjson.length();
            this->length = 0;
            this->val.string = (char*) malloc(ffjson.length());
            char * destination;
            while ((endIndex = ffjson.find("FFESCSTR", startIndex)) >= 0) {
                destination = this->val.string + this->length;
                memcpy(destination, ffjson.c_str() + startIndex, this->length += (endIndex - startIndex));
                startIndex = endIndex + 8;
            }
            endIndex = ffjson.length();
            destination = this->val.string + this->length;
            if (endIndex - startIndex > 0) {
                memcpy(destination, ffjson.c_str() + startIndex, this->length += (endIndex - startIndex));
            }
        } else {
            this->val.number = atof(ffjson.c_str());
            this->type = FFJSON_OBJ_TYPE::NUMBER;
        }
    }
}

FFJSON::~FFJSON() {
    if (this->type == FFJSON_OBJ_TYPE::OBJECT) {
        std::map<std::string, FFJSON*>::iterator i;
        i = val.pairs->begin();
        while (i != val.pairs->end()) {
            delete i->second;
            i++;
        }
        val.pairs->clear();
        delete this->val.pairs;
    } else if (this->type == FFJSON_OBJ_TYPE::ARRAY) {
        int i = this->val.array->size() - 1;
        while (i > 0) {
            delete (*this->val.array)[i];
            i--;
        }
        delete this->val.array;
    } else if (this->type == FFJSON_OBJ_TYPE::STRING || this->type == FFJSON_OBJ_TYPE::UNRECOGNIZED) {
        free(this->val.string);
    }
}

void FFJSON::trimWhites(std::string& s) {
    int i = 0;
    int j = s.length() - 1;
    while (s[i] == ' ' || s[i] == '\n' || s[i] == '\t') {
        i++;
    }
    while (s[j] == ' ' || s[j] == '\n' || s[j] == '\t') {
        j--;
    }
    j++;
    s = s.substr(i, j - i);
}

void FFJSON::trimQuotes(std::string& s) {
    int i = 0;
    int j = s.length() - 1;
    if (s[0] == '"') {
        i++;
    }
    if (s[j] == '"') {
        j--;
    }
    j++;
    s = s.substr(i, j - i);
}

FFJSON::FFJSON_OBJ_TYPE FFJSON::objectType(std::string ffjson) {
    if (ffjson[0] == '{' && ffjson[ffjson.length() - 1] == '}') {
        return FFJSON_OBJ_TYPE::OBJECT;
    } else if (ffjson[0] == '"' && ffjson[ffjson.length() - 1] == '"') {
        return FFJSON_OBJ_TYPE::STRING;
    } else if (ffjson[0] == '[' && ffjson[ffjson.length() - 1] == ']') {
        return FFJSON_OBJ_TYPE::ARRAY;
    } else if (ffjson.compare("true") == 0 || ffjson.compare("false") == 0) {
        return FFJSON_OBJ_TYPE::BOOL;
    } else {
        return FFJSON_OBJ_TYPE::UNRECOGNIZED;
    }
}

FFJSON& FFJSON::operator[](const char* prop) {
    return (*this)[std::string(prop)];
}

FFJSON& FFJSON::operator[](std::string prop) {
    if (this->type == FFJSON_OBJ_TYPE::OBJECT) {
        if ((*this->val.pairs).find(prop) != (*this->val.pairs).end()) {
            if ((*this->val.pairs)[prop] != NULL) {
                return *((*this->val.pairs)[prop]);
            } else {
                throw Exception("NULL");
            }
        } else {
            throw Exception("Key \"" + prop + "\" not defined");
        }
    } else {
        throw Exception("NON OBJECT TYPE");
    }
}

FFJSON& FFJSON::operator[](int index) {
    if (this->type == FFJSON_OBJ_TYPE::ARRAY) {
        if ((*this->val.array)[index] != NULL) {
            return *((*this->val.array)[index]);
        } else {
            throw Exception("NULL");
        }
    } else {
        throw Exception("NON ARRAY TYPE");
    }
};

/**
 * converts FFJSON object to json string
 * @param encode_to_base64 if true then the binary data is base64 encoded
 * @return json string of this FFJSON object
 */
std::string FFJSON::stringify() {
    if (this->type == FFJSON_OBJ_TYPE::STRING) {
        this->ffjson = "\"" + std::string(this->val.string, this->length) + "\"";
        return this->ffjson;
    } else if (this->type == FFJSON_OBJ_TYPE::NUMBER) {
        return (this->ffjson = std::string(std::to_string(this->val.number)));
    } else if (this->type == FFJSON_OBJ_TYPE::UNRECOGNIZED) {
        if (this->base64encode) {
            int output_length = 0;
            char * b64_char = base64_encode((const unsigned char*) this->val.string, this->length, (size_t*) & output_length);
            std::string b64_str(b64_char, output_length);
            return (this->ffjson = "\"" + b64_str + "\"");
        } else {
            return (this->ffjson = "\"" + std::string(this->val.string) + "\"");
        }
    } else if (this->type == FFJSON_OBJ_TYPE::BOOL) {
        if (this->val.boolean) {
            return (this->ffjson = "true");
        } else {
            return (this->ffjson = "false");
        }
    } else if (this->type == FFJSON_OBJ_TYPE::OBJECT) {
        std::map<std::string, FFJSON*>& objmap = *(this->val.pairs);
        this->ffjson = "{";
        std::map<std::string, FFJSON*>::iterator i;
        i = objmap.begin();
        while (i != objmap.end()) {
            if (this->base64encode)i->second->base64encode = true;
            if (this->base64encodeChildren&&!this->base64encodeStopChain)i->second->base64encodeChildren = true;
            this->ffjson.append("\"" + i->first + "\":");
            this->ffjson.append(i->second->stringify());
            this->ffjson.append(",");
            i++;
        }
        if (objmap.size() == 0) {
            this->ffjson += ',';
        }
        this->ffjson[this->ffjson.length() - 1] = '}';
        return this->ffjson;
    } else if (this->type == FFJSON_OBJ_TYPE::ARRAY) {
        std::vector<FFJSON*>& objarr = *(this->val.array);
        this->ffjson = "[";
        int i = 0;
        while (i < objarr.size()) {
            if (this->base64encode)objarr[i]->base64encode = true;
            if (this->base64encodeChildren&&!this->base64encodeStopChain)objarr[i]->base64encodeChildren = true;
            this->ffjson.append(objarr[i]->stringify());
            this->ffjson.append(",");
            i++;
        }
        if (objarr.size() == 0) {
            this->ffjson += ',';
        }
        this->ffjson[this->ffjson.length() - 1] = ']';
        return this->ffjson;
    }
}

/**
 */
FFJSON::operator const char*() {
    return this->ffjson.c_str();
}

FFJSON::operator double() {
    return this->val.number;
}

FFJSON::operator bool() {
    return this->val.boolean;
}

FFJSON::operator int() {
    return (int) this->val.number;
}

FFJSON::operator unsigned int() {
    return (unsigned int) this->val.number;
}
