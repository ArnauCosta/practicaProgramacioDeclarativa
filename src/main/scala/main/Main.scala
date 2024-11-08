package main

import java.io.File
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import mapreduce._

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.math.sqrt

// Tenim dos objectes executables
// - tractaxml utilitza un "protoParser" per la viquipedia i
// exampleMapreduce usa el MapReduce

object Main extends App {


  def freq(text: String): List[(String, Int)] = {
    // Normalitzem el text a minúscules i eliminen caracters no alfabetics ni espais
    val textNormalitzat = text.toLowerCase.replaceAll("[^a-z\\s]", "")
    // Separem el text en paraules (utilitzant espais com a separador)
    val paraules = textNormalitzat.split("\\s+")

    // Comptem les frequencies de les paraules utilitzant foldLeft
    val comptesParaules = paraules.foldLeft(Map.empty[String, Int]) { (acc, paraula) =>
      // Actualitzem el compte de cada paraula
      acc + (paraula -> (acc.getOrElse(paraula, 0) + 1))
    }

    // Convertim el Map a una llista i la ordenem per frequencia de major a menor
    val llistaParaules = comptesParaules.toList
    val llistaOrdenadaParaules = llistaParaules.sortBy(-_._2)
    llistaOrdenadaParaules
  }

  def nonstopfreq(text: String, nonstopWords: Set[String]): List[(String, Int)] = {
    // Normalitzem el text a minúscules i eliminen caracters no alfabetics ni espais
    val textNormalitzat = text.toLowerCase.replaceAll("[^a-z\\s]", "")
    // Separem el text en paraules, eliminant les paraules de parada i les paraules buides
    val paraules = textNormalitzat.split("\\s+").filterNot(nonstopWords.contains).filter(_.nonEmpty)

    // Comptem les frequencies de les paraules utilitzant foldLeft
    val comptesParaules = paraules.foldLeft(Map.empty[String, Int]) { (acc, paraula) =>
      // Actualitzem el compte de cada paraula
      acc.updated(paraula, acc.getOrElse(paraula, 0) + 1)
    }

    // Convertim el Map a una llista i la ordenem per frequencia de major a menor
    comptesParaules.toList.sortBy(-_._2)
  }

  def paraulafreqfreq(text: String): Unit = {
    // Normalitzem el text a minúscules i eliminen caracters no alfabetics ni espais
    val textNormalitzat = text.toLowerCase.replaceAll("[^a-z\\s]", "")
    // Separem el text en paraules
    val paraules = textNormalitzat.split("\\s+")

    // Comptem les frequencies de les paraules utilitzant foldLeft
    val comptesParaules = paraules.foldLeft(Map.empty[String, Int]) { (acc, paraula) =>
      // Actualitzem el compte de cada paraula
      acc.updated(paraula, acc.getOrElse(paraula, 0) + 1)
    }

    // Comptem la frequencia de les frequencies (quantes vegades apareix cada frequencia)
    val comptesFrequencies = comptesParaules.values.groupBy(identity).view.mapValues(_.size).toMap

    // Ordenem les frequencies per aparició de major a menor
    val comptesFrequenciesOrdenades = comptesFrequencies.toList.sortBy(-_._2)

    // Obtenim les 10 frequencies més frequents
    val top10Frequencies = comptesFrequenciesOrdenades.take(10)

    // Obtenim les 5 frequencies menys frequents
    val bot5Frequencies = comptesFrequenciesOrdenades.reverse.take(5)

    // Mostrem els resultats
    println("Les 10 frequencies més frequents:")
    top10Frequencies.foreach { case (frequencia, comptatge) =>
      println(s"$comptatge paraules apareixen $frequencia vegades")
    }

    println("\nLes 5 frequencies menys frequents:")
    bot5Frequencies.foreach { case (frequencia, comptatge) =>
      println(s"$comptatge paraules apareixen $frequencia vegades")
    }
  }

  def ngram(text: String, n: Int): List[(String, Int)] = {
    // Normalitzem el text i l'convertim a minúscules, mantenint només lletres i espais
    val textNormalitzat = text.toLowerCase.replaceAll("[^a-z\\s]", "")

    // Separem el text en paraules, eliminant les buides
    val paraules = textNormalitzat.split("\\s+")

    // Generem els n-grams, combinant paraules consecutives de mida n
    val ngrams = paraules.sliding(n).map(_.mkString(" ")).toList

    // Comptem la freqüència de cada n-gram utilitzant foldLeft
    val comptesNgrams = ngrams.foldLeft(Map.empty[String, Int]) { (acc, ngram) =>
      acc.updated(ngram, acc.getOrElse(ngram, 0) + 1)
    }

    // Convertim el Map a una llista i la ordenem per freqüència, de major a menor
    val llistaOrdenadaNgrams = comptesNgrams.toList.sortBy(-_._2)

    llistaOrdenadaNgrams
  }

  def normalitzaFrequencies(comptesParaules: List[(String, Int)]): Map[String, Double] = {
    // Obtenim la maxima frequencia per normalitzar
    val maximaFrequencia = comptesParaules.headOption.map(_._2).getOrElse(1)

    // Normalitzem les frequencies dividint cada frequencia per la maxima
    comptesParaules.map { case (paraula, comptatge) =>
      paraula -> (comptatge.toDouble / maximaFrequencia)
    }.toMap
  }

  def cosinesim(text1: String, text2: String, paraulesDeAturada: Set[String], n: Int = 1): Double = {

    // Calculem les frequencies normalitzades per a ambdós textos, excloent les paraules de parada
    val freq1 = nonstopfreq(text1, paraulesDeAturada)
    val freq2 = nonstopfreq(text2, paraulesDeAturada)

    // Normalitzem les frequencies per a cada text
    val freqNormalitzada1 = normalitzaFrequencies(freq1)
    val freqNormalitzada2 = normalitzaFrequencies(freq2)

    // Obtenim el conjunt de totes les paraules que apareixen en ambdós textos
    val totesLesParaules = (freqNormalitzada1.keySet ++ freqNormalitzada2.keySet).toList

    // Creem els vectors de frequencies per a cada text
    val vector1 = totesLesParaules.map(paraula => freqNormalitzada1.getOrElse(paraula, 0.0))
    val vector2 = totesLesParaules.map(paraula => freqNormalitzada2.getOrElse(paraula, 0.0))

    // Calcula el producte punt entre els dos vectors
    val productePunt = vector1.zip(vector2).map { case (a, b) => a * b }.sum

    // Calcula la magnitud de cada vector
    val magnitud1 = sqrt(vector1.map(x => x * x).sum)
    val magnitud2 = sqrt(vector2.map(x => x * x).sum)

    // Si alguna magnitud és 0, retornem 0 (per evitar divisió per 0)
    if (magnitud1 == 0 || magnitud2 == 0) 0.0
    else productePunt / (magnitud1 * magnitud2)
  }

  val text = ProcessListStrings.llegirFitxer("primeraPartPractica/pg11.txt")
  val text2 = ProcessListStrings.llegirFitxer("primeraPartPractica/pg12.txt")
  val nonstopWords = ProcessListStrings.llegirFitxer("primeraPartPractica/english-stop.txt").split("\n").toSet

  //println(cosinesim(text, text2, nonstopWords))


  //val topNgrams = nonstopfreq(text, nonstopWords).take(10)
  //val topNgrams = freq(text).take(10)
  val topNgrams = ngram(text, 4).take(10)

//  println(s"Les 10 n-grammes més frequents:")
//  topNgrams.foreach { case (ngram, count) =>
//    println(f"$ngram%-30s $count%5d")
//  }

  //paraulafreqfreq(text)


//  val wordFreqs = nonstopfreq(text, nonstopWords)
//  print(wordFreqs)
//  val totalWords = wordFreqs.map(_._2).sum
//  val uniqueWords = wordFreqs.length
//  println(s"Num de Paraules: $totalWords Diferents: $uniqueWords")
//  println("Paraules ocurrencies frequencia")
//  println("---------------------------------------")
//
//  wordFreqs.take(10).foreach { case (word, count) =>
//    val frequency = (count.toDouble / totalWords) * 100
//    println(f"$word%-10s $count%-10d ${frequency}%.2f")
//  }


  val strings = List("pg11", "pg11-net", "pg12", "pg12-net", "pg74", "pg74-net", "pg2500", "pg2500-net")

  for {
    i <- strings.indices
    j <- i + 1 until strings.length
  } {
    val text1 = ProcessListStrings.llegirFitxer(s"primeraPartPractica/${strings(i)}.txt")
    val text2 = ProcessListStrings.llegirFitxer(s"primeraPartPractica/${strings(j)}.txt")

    val similarity = cosinesim(text1, text2, nonstopWords)

    println(s"Similaritat entre ${strings(i)} i ${strings(j)}: $similarity")
  }


}

object fitxers extends App{
  ProcessListStrings.mostrarTextDirectori("primeraPartPractica")
}

object tractaxml extends App {

  val parseResult= ViquipediaParse.parseViquipediaFile()

  parseResult match {
    case ViquipediaParse.ResultViquipediaParsing(t,c,r) =>
      println("TITOL: "+ t)
      println("CONTINGUT: ")
      println(c)
      println("REFERENCIES: ")
      println(r)
  }
}

object exampleMapreduce extends App {

  val nmappers = 1
  val nreducers = 1
  val f1 = new java.io.File("f1")
  val f2 = new java.io.File("f2")
  val f3 = new java.io.File("f3")
  val f4 = new java.io.File("f4")
  val f5 = new java.io.File("f5")
  val f6 = new java.io.File("f6")
  val f7 = new java.io.File("f7")
  val f8 = new java.io.File("f8")

  val fitxers: List[(File, List[String])] = List(
    (f1, List("hola", "adeu", "per", "palotes", "hola","hola", "adeu", "pericos", "pal", "pal", "pal")),
    (f2, List("hola", "adeu", "pericos", "pal", "pal", "pal")),
    (f3, List("que", "tal", "anem", "be")),
    (f4, List("be", "tal", "pericos", "pal")),
    (f5, List("doncs", "si", "doncs", "quin", "pal", "doncs")),
    (f6, List("quin", "hola", "vols", "dir")),
    (f7, List("hola", "no", "pas", "adeu")),
    (f8, List("ahh", "molt", "be", "adeu")))


  val compres: List[(String,List[(String,Double, String)])] = List(
    ("bonpeu",List(("pep", 10.5, "1/09/20"), ("pep", 13.5, "2/09/20"), ("joan", 30.3, "2/09/20"), ("marti", 1.5, "2/09/20"), ("pep", 10.5, "3/09/20"))),
    ("sordi", List(("pep", 13.5, "4/09/20"), ("joan", 30.3, "3/09/20"), ("marti", 1.5, "1/09/20"), ("pep", 7.1, "5/09/20"), ("pep", 11.9, "6/09/20"))),
    ("canbravo", List(("joan", 40.4, "5/09/20"), ("marti", 100.5, "5/09/20"), ("pep", 10.5, "7/09/20"), ("pep", 13.5, "8/09/20"), ("joan", 30.3, "7/09/20"), ("marti", 1.5, "6/09/20"))),
    ("maldi", List(("pepa", 10.5, "3/09/20"), ("pepa", 13.5, "4/09/20"), ("joan", 30.3, "8/09/20"), ("marti", 0.5, "8/09/20"), ("pep", 72.1, "9/09/20"), ("mateu", 9.9, "4/09/20"), ("mateu", 40.4, "5/09/20"), ("mateu", 100.5, "6/09/20")))
  )

  // Creem el sistema d'actors
  val systema: ActorSystem = ActorSystem("sistema")

  // funcions per poder fer un word count
  def mappingWC(file:File, words:List[String]) :List[(String, Int)] =
        for (word <- words) yield (word, 1)


  def reducingWC(word:String, nums:List[Int]):(String,Int) =
        (word, nums.sum)


  println("Creem l'actor MapReduce per fer el wordCount")
  val wordcount = systema.actorOf(Props(new MapReduce(fitxers,mappingWC,reducingWC )), name = "mastercount")

  // Els Futures necessiten que se'ls passi un temps d'espera, un pel future i un per esperar la resposta.
  // La idea és esperar un temps limitat per tal que el codi no es quedés penjat ja que si us fixeu preguntar
  // i esperar denota sincronització. En el nostre cas, al saber que el codi no pot avançar fins que tinguem
  // el resultat del MapReduce, posem un temps llarg (100000s) al preguntar i una Duration.Inf a l'esperar la resposta.

  // Enviem un missatge com a pregunta (? enlloc de !) per tal que inicii l'execució del MapReduce del wordcount.
  //var futureresutltwordcount = wordcount.ask(mapreduce.MapReduceCompute())(100000 seconds)

  implicit val timeout = Timeout(10000 seconds) // L'implicit permet fixar el timeout per a la pregunta que enviem al wordcount. És obligagori.
  var futureresutltwordcount = wordcount ? mapreduce.MapReduceCompute()

  println("Awaiting")
  // En acabar el MapReduce ens envia un missatge amb el resultat
  val wordCountResult:Map[String,Int] = Await.result(futureresutltwordcount,Duration.Inf).asInstanceOf[Map[String,Int]]


  println("Results Obtained")
  for(v<-wordCountResult) println(v)

  // Fem el shutdown del actor system
  println("shutdown")
  systema.terminate()
  println("ended shutdown")
  // com tancar el sistema d'actors.

  /*
  EXERCICIS:

  Useu el MapReduce per saber quant ha gastat cada persona.

  Useu el MapReduce per saber qui ha fet la compra més cara a cada supermercat

  Useu el MapReduce per saber quant s'ha gastat cada dia a cada supermercat.
   */


  println("tot enviat, esperant... a veure si triga en PACO")
}



