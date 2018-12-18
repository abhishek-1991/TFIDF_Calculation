/**********************************************************
 * Author Info:
 * asrivas3 Abhishek Kumar Srivastava
 * CSC 548 Assignment#6 Problem#1
 **********************************************************/

#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<dirent.h>
#include<math.h>
#include<mpi.h>

#define MAX_WORDS_IN_CORPUS 32
#define MAX_FILEPATH_LENGTH 16
#define MAX_WORD_LENGTH 16
#define MAX_DOCUMENT_NAME_LENGTH 8
#define MAX_STRING_LENGTH 64

typedef char word_document_str[MAX_STRING_LENGTH];

typedef struct o {
	char word[32];
	char document[8];
	int wordCount;
	int docSize;
	int numDocs;
	int numDocsWithWord;
	double tfidf_val;
} obj;

typedef struct w {
	char word[32];
	int numDocsWithWord;
	int currDoc;
} u_w;

static int myCompare (const void * a, const void * b)
{
    return strcmp (a, b);
}

int main(int argc , char *argv[]){
	DIR* files;
	struct dirent* file;
	int i,j;
	int numDocs = 0, docSize, contains;
	char filename[MAX_FILEPATH_LENGTH], word[MAX_WORD_LENGTH], document[MAX_DOCUMENT_NAME_LENGTH];
	
	// Will hold all TFIDF objects for all documents
	obj TFIDF[MAX_WORDS_IN_CORPUS];
	int TF_idx = 0;
	
	// Will hold all unique words in the corpus and the number of documents with that word
	u_w unique_words[MAX_WORDS_IN_CORPUS];
	int uw_idx = 0;
	
	// Will hold the final strings that will be printed out
	word_document_str strings[MAX_WORDS_IN_CORPUS];
	
	// Variables defined for MPI functions
	int numOfProcs, myRank;

	// Initialzed MPI, stored number of nodes and rank of the current node
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &numOfProcs);
	MPI_Comm_rank(MPI_COMM_WORLD, &myRank);

	// Defined variables for storing values at the root node
	u_w uniqueWordsRoot[MAX_WORDS_IN_CORPUS * numOfProcs];
	obj TFIDFRoot[MAX_WORDS_IN_CORPUS * numOfProcs];
	int wordCountRoot = 0, counter = 0;
	
	// Initialized the variables
	for(i = 0;i<MAX_WORDS_IN_CORPUS;i++)
		TFIDF[i].numDocsWithWord = -1;
	
	for(i = 0;i<MAX_WORDS_IN_CORPUS;i++)
		unique_words[i].numDocsWithWord = -1;

	//Count number of documents at the root
	if (myRank == 0)
	{
		if ((files = opendir("input")) == NULL) {
			printf("Directory failed to open\n");
			exit(1);
		}
		while ((file = readdir(files)) != NULL) {
			// On linux/Unix we don't want current and parent directories
			if (!strcmp(file->d_name, "."))	 continue;
			if (!strcmp(file->d_name, "..")) continue;
			numDocs++;
		}
	}
	//printf("DEBUG 1: %d\n",myRank);
	
	// Send the number of documents to all the worker nodes
	MPI_Bcast(&numDocs, 1, MPI_INT, 0, MPI_COMM_WORLD);
	
	// Workers read the documents assigned to them
	if (myRank != 0)
	{
		// Loop through each document and gather TFIDF variables for each word
		for (i = myRank; i <= numDocs; i+=(numOfProcs - 1)) {
			sprintf(document, "doc%d", i);
			sprintf(filename, "input/%s", document);
			FILE* fp = fopen(filename, "r");
			if (fp == NULL) {
				printf("Error Opening File: %s\n", filename);
				exit(0);
			}

			// Get the document size
			docSize = 0;
			while ((fscanf(fp, "%s", word)) != EOF)
				docSize++;

			// For each word in the document
			fseek(fp, 0, SEEK_SET);
			while ((fscanf(fp, "%s", word)) != EOF) {
				contains = 0;

				// If TFIDF array already contains the word@document, just increment wordCount and break
				for (j = 0; j < TF_idx; j++) {
					if (!strcmp(TFIDF[j].word, word) && !strcmp(TFIDF[j].document, document)) {
						contains = 1;
						TFIDF[j].wordCount++;
						break;
					}
				}

				//If TFIDF array does not contain it, make a new one with wordCount=1
				if (!contains) {
					strcpy(TFIDF[TF_idx].word, word);
					strcpy(TFIDF[TF_idx].document, document);
					TFIDF[TF_idx].wordCount = 1;
					TFIDF[TF_idx].docSize = docSize;
					TFIDF[TF_idx].numDocs = numDocs;
					TF_idx++;
				}

				contains = 0;
				// If unique_words array already contains the word, just increment numDocsWithWord
				for (j = 0; j < uw_idx; j++) {
					if (!strcmp(unique_words[j].word, word)) {
						contains = 1;
						if (unique_words[j].currDoc != i) {
							unique_words[j].numDocsWithWord++;
							unique_words[j].currDoc = i;
						}
						break;
					}
				}

				// If unique_words array does not contain it, make a new one with numDocsWithWord=1 
				if (!contains) {
					strcpy(unique_words[uw_idx].word, word);
					unique_words[uw_idx].numDocsWithWord = 1;
					unique_words[uw_idx].currDoc = i;
					uw_idx++;
				}
			}
			fclose(fp);
		}
	}
	//printf("DEBUG 2: %d\n",myRank);
	
	// wait until all the workers have finished reading
	MPI_Barrier(MPI_COMM_WORLD);

	// unique words are transfered to root node form the worker nodes
	MPI_Gather(&unique_words, sizeof(u_w) * MAX_WORDS_IN_CORPUS, MPI_BYTE, &uniqueWordsRoot, sizeof(u_w) * MAX_WORDS_IN_CORPUS, MPI_BYTE,0, MPI_COMM_WORLD);

	// Print TF job similar to HW4/HW5 (For debugging purposes)
	/*printf("-------------TF Job-------------\n");
	for(j=0; j<TF_idx; j++)
		printf("%s@%s\t%d/%d\n", TFIDF[j].word, TFIDF[j].document, TFIDF[j].wordCount, TFIDF[j].docSize);*/

	// root node processes the unique words
	if (myRank == 0)
	{
		for (i = 0; i < MAX_WORDS_IN_CORPUS * numOfProcs; i++)
		{
			if (uniqueWordsRoot[i].numDocsWithWord != -1)
			{
				for (j = i + 1; j < MAX_WORDS_IN_CORPUS * numOfProcs; j++)
				{
					if (uniqueWordsRoot[j].numDocsWithWord != -1 && strcmp(uniqueWordsRoot[j].word, uniqueWordsRoot[i].word) == 0)
					{
						uniqueWordsRoot[i].numDocsWithWord =  uniqueWordsRoot[i].numDocsWithWord +  uniqueWordsRoot[j].numDocsWithWord;
						uniqueWordsRoot[j].numDocsWithWord = -1;
					}
				}
			}
		}

		for (i = 0; i < MAX_WORDS_IN_CORPUS * numOfProcs; i++)
		{
			if (uniqueWordsRoot[i].numDocsWithWord != -1)
			{
				unique_words[wordCountRoot].numDocsWithWord = uniqueWordsRoot[i].numDocsWithWord;
				strcpy(unique_words[wordCountRoot].word, uniqueWordsRoot[i].word);
				wordCountRoot++;
			}
		}
	}
	//printf("DEBUG 3: %d\n",myRank);

	// unique words are broadcasted to the worker nodes
	MPI_Bcast(&unique_words, sizeof(u_w) * MAX_WORDS_IN_CORPUS, MPI_BYTE, 0, MPI_COMM_WORLD);
	
	// worker nodes calculate the TFIDF values
	if (myRank != 0)
	{
		// Use unique_words array to populate TFIDF objects with: numDocsWithWord
		for (i = 0; i < TF_idx; i++) {
			for (j = 0; j < MAX_WORDS_IN_CORPUS; j++) {
				if (!strcmp(TFIDF[i].word, unique_words[j].word)) {
					TFIDF[i].numDocsWithWord = unique_words[j].numDocsWithWord;
					break;
				}
			}
		}

		// Print IDF job similar to HW4/HW5 (For debugging purposes)
		/*printf("------------IDF Job-------------\n");
		for(j=0; j<TF_idx; j++)
			printf("%s@%s\t%d/%d\n", TFIDF[j].word, TFIDF[j].document, TFIDF[j].numDocs, TFIDF[j].numDocsWithWord);*/

			// Calculates TFIDF value and puts: "document@word\tTFIDF" into strings array
		for (j = 0; j < TF_idx; j++) {
			double TF = 1.0 * TFIDF[j].wordCount / TFIDF[j].docSize;
			double IDF = log(1.0 * TFIDF[j].numDocs / TFIDF[j].numDocsWithWord);
			TFIDF[j].tfidf_val = TF * IDF;
		}
	}
	//printf("DEBUG 4: %d\n",myRank);
	
	// workers send the TFIDF values to root node
	MPI_Gather(&TFIDF, sizeof(obj) * MAX_WORDS_IN_CORPUS, MPI_BYTE, &TFIDFRoot, sizeof(obj) * MAX_WORDS_IN_CORPUS, MPI_BYTE, 0, MPI_COMM_WORLD);

	// root node prints the result in the file
	if (myRank == 0)
	{
		for (i = 0; i < MAX_WORDS_IN_CORPUS * numOfProcs; i++)
		{
			if (TFIDFRoot[i].numDocsWithWord != -1)
			{
				sprintf(strings[counter], "%s@%s\t%.16f", TFIDFRoot[i].document, TFIDFRoot[i].word, TFIDFRoot[i].tfidf_val);
				//printf("%s\n",strings[i]);
				counter++;
			}
		}

		// Sort strings and print to file
		qsort(strings, counter, sizeof(char)*MAX_STRING_LENGTH, myCompare);
		FILE* fp = fopen("output.txt", "w");
		if (fp == NULL) {
			printf("Error Opening File: output.txt\n");
			exit(0);
		}
		for (i = 0; i < counter; i++)
			fprintf(fp, "%s\n", strings[i]);
		fclose(fp);
	}
	//printf("DEBUG 5: %d\n",myRank);
	
	// Closed the MPI connections
	MPI_Finalize();
	return 0;	
}
