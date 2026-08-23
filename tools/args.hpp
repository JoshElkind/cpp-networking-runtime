#pragma once

#include <cstdio>
#include <cstdlib>
#include <map>
#include <stdexcept>
#include <string>
#include <string_view>

namespace nmr::tools {

// Minimal --key=value / --key value / --flag parser. Unknown keys are an error
// so a typo doesn't silently run the default.
class Args {
 public:
  Args(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
      std::string_view a = argv[i];
      if (a.substr(0, 2) != "--") throw std::invalid_argument("unexpected argument: " + std::string(a));
      a.remove_prefix(2);
      if (const auto eq = a.find('='); eq != std::string_view::npos) {
        values_[std::string(a.substr(0, eq))] = std::string(a.substr(eq + 1));
      } else if (i + 1 < argc && std::string_view(argv[i + 1]).substr(0, 2) != "--") {
        values_[std::string(a)] = argv[++i];
      } else {
        values_[std::string(a)] = "true";
      }
    }
  }

  bool has(const std::string& key) {
    used_[key] = true;
    return values_.count(key) != 0;
  }

  std::string get(const std::string& key, const std::string& def) {
    used_[key] = true;
    const auto it = values_.find(key);
    return it == values_.end() ? def : it->second;
  }

  long long get_int(const std::string& key, long long def) {
    const std::string v = get(key, "");
    return v.empty() ? def : std::stoll(v);
  }

  double get_double(const std::string& key, double def) {
    const std::string v = get(key, "");
    return v.empty() ? def : std::stod(v);
  }

  // Call after every get(); rejects anything the program didn't ask for.
  void check_unused() const {
    for (const auto& [k, v] : values_) {
      if (used_.count(k) == 0) throw std::invalid_argument("unknown option --" + k);
    }
  }

 private:
  std::map<std::string, std::string> values_;
  std::map<std::string, bool> used_;
};

}  // namespace nmr::tools
