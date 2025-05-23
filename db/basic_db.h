//
//  basic_db.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_BASIC_DB_H_
#define YCSB_C_BASIC_DB_H_
#define HALF_MAX_NUM_KEYS 16

#include "core/db.h"
#include "BTree/include/fc/btree.h"

#include <iostream>
#include <string>
#include <mutex>
#include "core/properties.h"

using std::cout;
using std::endl;

namespace ycsbc {

class BasicDB : public DB {
 private:
    frozenca::BTreeSet<std::string, HALF_MAX_NUM_KEYS> bt;
 public:
  void Init() {
    std::lock_guard<std::mutex> lock(mutex_);
    cout << "A new thread begins working." << endl;
  }

  void DestoryTree() {
    bt.clear();
  }

  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result) {
    std::lock_guard<std::mutex> lock(mutex_);
    /*cout << "READ " << table << ' ' << key;
    if (fields) {
      cout << " [ ";
      for (auto f : *fields) {
        cout << f << ' ';
      }
      cout << ']' << endl;
    } else {
      cout  << " < all fields >" << endl;
    }*/
    //cout << "READ " << key << endl;
    if (!bt.contains(key)) {
      cout << "NOT FOUND" << endl;
    }
    return 0;
  }

  int Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result) {
    std::lock_guard<std::mutex> lock(mutex_);
    /*cout << "SCAN " << table << ' ' << key << " " << len;
    if (fields) {
      cout << " [ ";
      for (auto f : *fields) {
        cout << f << ' ';
      }
      cout << ']' << endl;
    } else {
      cout  << " < all fields >" << endl;
    }*/
    cout << "SCAN " << key << endl;
    return 0;
  }

  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {
    std::lock_guard<std::mutex> lock(mutex_);
    /*cout << "UPDATE " << table << ' ' << key << " [ ";
    for (auto v : values) {
      cout << v.first << '=' << v.second << ' ';
    }
    cout << ']' << endl;*/
    cout << "UPDATE " << key << endl;
    return 0;
  }

  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {
    std::lock_guard<std::mutex> lock(mutex_);
    /*cout << "INSERT " << key << " [ ";
    for (auto v : values) {
      cout << v.first << '=' << v.second << ' ';
    }
    cout << ']' << endl;*/
    // Commented out to avoid logging in loading phase
    //cout << "INSERT " << key << endl;
    bt.insert(key);
    if (bt.size() % 1000000 == 0) {
      cout << "bt.size(): " << bt.size() << endl;
    }
    return 0;
  }

  int Delete(const std::string &table, const std::string &key) {
    std::lock_guard<std::mutex> lock(mutex_);
    //cout << "DELETE " << table << ' ' << key << endl;
    cout << "DELETE " << key << endl;
    return 0; 
  }

 private:
  std::mutex mutex_;
};

} // ycsbc

#endif // YCSB_C_BASIC_DB_H_

