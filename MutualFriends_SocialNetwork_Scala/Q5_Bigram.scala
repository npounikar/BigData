 def stem(word: String) ={
             if(word.toLowerCase().endsWith("ness")) 
                    word.replace(word, word.substring(0, word.length()-3))
             else if(word.toLowerCase().endsWith("tion")) 
                    word.replace(word, word.substring(0, word.length()-4)) 
             else if(word.toLowerCase().endsWith("sion")) 
                    word.replace(word, word.substring(0, word.length()-4)) 
             else if(word.toLowerCase().endsWith("iness")) 
                    word.replace(word, word.substring(0, word.length()-5)+"y") 
             else if(word.toLowerCase().endsWith("er")) 
                    word.replace(word, word.substring(0, word.length()-2))
             else if(word.toLowerCase().endsWith("or")) 
                    word.replace(word, word.substring(0, word.length()-2)) 
             else if(word.toLowerCase().endsWith("ily")) 
                     word.replace(word, word.substring(0, word.length()-3)+"y") 
             else if(word.toLowerCase().endsWith("ily")) 
                     word.replace(word, word.substring(0, word.length()-3)+"y") 
             else if(word.toLowerCase().endsWith("ist")) 
                     word.replace(word, word.substring(0, word.length()-3))
             else if(word.toLowerCase().endsWith("ize")) 
                     word.replace(word, word.substring(0, word.length()-3)) 
             else if(word.toLowerCase().endsWith("en")) 
                    word.replace(word, word.substring(0, word.length()-2)) 
             else if(word.toLowerCase().endsWith("ful")) 
                     word.replace(word, word.substring(0, word.length()-3)) 
             else if(word.toLowerCase().endsWith("full")) 
                     word.replace(word, word.substring(0, word.length()-4)) 
             else if(word.toLowerCase().endsWith("ical")) 
                     word.replace(word, word.substring(0, word.length()-4)) 
             else if(word.toLowerCase().endsWith("ic")) 
                     word.replace(word, word.substring(0, word.length()-2)) 
             else if(word.toLowerCase().endsWith("sses")) 
                    word.replace(word, word.substring(0, word.length()-4)+"ss") 
             else if(word.toLowerCase().endsWith("ies")) 
                     word.replace(word, word.substring(0, word.length()-3)+"i") 
             else if(word.toLowerCase().endsWith("ss")) 
                     word.replace(word, word.substring(0, word.length()-2)+"ss") 
             else if(word.toLowerCase().endsWith("s")) 
                     word.replace(word, word.substring(0, word.length()-1))
             else if(word.toLowerCase().endsWith("eed")) 
                     word.replace(word, word.substring(0, word.length()-3)+"ed") 
             else if(word.toLowerCase().endsWith("ed")) 
                     word.replace(word, word.substring(0, word.length()-2))
             else if(word.toLowerCase().endsWith("ing")) 
                     word.replace(word, word.substring(0, word.length()-3)) 
             else if(word.toLowerCase().endsWith("ly")) 
                     word.replace(word, word.substring(0, word.length()-2)) 
             else if(word.toLowerCase().endsWith("es")) 
                    word.replace(word, word.substring(0, word.length()-2))
      
             else 
                 word
           
      } 
        

 val inputString = "Alice is testing spark application. Testing spark is fun".toLowerCase()
      //val s = input.toString().toLowerCase()
      //val inputarray = input.collect()
      //val sarr = inputarray.mkString(" ")
      //val inputString = sarr.toLowerCase()
      val stopWords = "a,able,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,be,because,been,but,by,can,cannot,could,dear,did,do,does,either,else,ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,say,says,she,should,since,so,some,than,that,the,their,them,then,there,these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,which,while,who,whom,why,will,with,would,yet,you,your"
      val stopWordsArray = stopWords.split(",")     
      val words = inputString.split("""[\?\.\!",\- \s]+""")
      var finalOutputString = new Array[String](words.length)
      var answer = ""
      var flag = 0;
      for(a <-0 to words.length - 1){
        val stemmedWord = stem(words(a))
        for(b <- 0 to stopWordsArray.length - 1){
              if (stemmedWord.toString() == stopWordsArray(b).toString()){//words(a)
                flag = 1
                finalOutputString(a)= "be"
              }
            }
            if(flag == 0){
              finalOutputString(a)= stemmedWord;
            }
            flag = 0
            
      }
      
      println(finalOutputString.mkString(" "))
      
      val biGrams = finalOutputString.sliding(2).toSeq
      val count = for((words, occurences) <- biGrams.groupBy(_.toSeq).toSeq) 
                    yield occurences.size -> words.mkString("','")
      val sortedOutput = count.sorted(Ordering.by((_: (Int, String))._1).reverse)
     
      println("('"+sortedOutput(0)._2+"')" + "\t" + sortedOutput(0)._1)
      