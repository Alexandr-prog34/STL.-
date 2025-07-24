#include <iostream>

#include <processing.h>

/*
Программа считает частоту слов, встречающихся во всех файлах в директории 
и выводит итоговую статистику в консоль
*/

// int main(int argc, char **argv) {
//   if(agrc != 2)
//     return 1;
  

//   bool recursive = false;
//   Dir(argv[1], recursive) 
//     | Filter([](std::filesystem::path& p){ return p.extension() == ".txt"; })
//     | OpenFiles()
//     | Split("\n ,.;")
//     | Transform(
//         [](std::string& token) { 
//             std::transform(token.begin(), token.end(), token.begin(), [](char c){return std::tolower(c);});
//             return token;
//         })
//     | AggregateByKey(
//         0uz, 
//         [](const std::string&, size_t& count) { ++count;},
//         [](const std::string& token) { return token;}
//       )
//     | Transform([](const std::pair<std::string, size_t>& stat) { return std::format("{} - {}", stat.first, stat.second);})
//     | Out(std::cout);

  
//   return 0;
// }

int main(){
  return 0;
}
