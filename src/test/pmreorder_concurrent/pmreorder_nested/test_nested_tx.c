/*
 * test_nested_tx.c -- unit test for undo log recovery
 */
#include "unittest.h"
#include "util.h"
#include "valgrind_internal.h"
#include <string.h>

#define TEST_VALUE_A 5
#define TEST_VALUE_B 10
#define TEST_VALUE_C 15

TOID_DECLARE_ROOT(struct test_obj);

struct test_obj {
	int a;
	int b;
	int c;
};


// pmreorder_create_pool -- create a obj pool before recording ops
//
// Two parameters required
// 1 - the path to the testfile
// 2 - The layout of the pool
static void
 pmreorder_create_pool(char* file, char* layout){
       //Create theobject pool
        PMEMobjpool *pop;
        if ((pop = pmemobj_create(file, layout, PMEMOBJ_MIN_POOL, 777)) == NULL) {
                printf("Error : Exists");
                UT_FATAL("!pmemobj_create : %s", file);
        }
	
	TOID(struct test_obj) obj = (TOID(struct test_obj))pmemobj_root(pop, sizeof (struct test_obj));
        //POBJ_ZNEW(pop, &obj, struct test_obj);

        D_RW(obj)->a = 1;
        D_RW(obj)->b = 1;
        D_RW(obj)->c = 1;

	printf("Done creating pool");
	pmemobj_close(pop);
}


static void
do_tx_macro_commit_nested(char* file, char* layout)
{

	//Open the object pool
        PMEMobjpool *pop;
        if ((pop = pmemobj_open(file, layout)) == NULL) {
                printf("Error :File doesn't exist");
                UT_FATAL("!pmemobj_open : %s", file);
        }

	//Get the root object that we will modify
	TOID(struct test_obj) obj = (TOID(struct test_obj))pmemobj_root(pop, sizeof (struct test_obj));


	VALGRIND_EMIT_LOG("WORKLOAD.BEGIN");
	VALGRIND_EMIT_LOG("TX1.BEGIN");
	TX_BEGIN(pop) {
		TX_ADD(obj);
		TX_BEGIN(pop) {
			D_RW(obj)->a = TEST_VALUE_A;
		} TX_ONCOMMIT {
			UT_ASSERT(D_RW(obj)->a == TEST_VALUE_A);
			D_RW(obj)->b = TEST_VALUE_B;
			D_RW(obj)->c = TEST_VALUE_C;
		} TX_END
	} TX_ONCOMMIT {
	} TX_END

	VALGRIND_EMIT_LOG("TX1.END");
	VALGRIND_EMIT_LOG("WORKLOAD.END");
	printf ("On commit tx values are a:%d, b%d, C:%d", D_RW(obj)->a, D_RW(obj)->b, D_RW(obj)->c);
	pmemobj_close(pop);

}


// TEST 2 : Modify a field in the oncommit block - 
static void
do_tx_macro_commit_nested_nopersist_c(char* file, char* layout)
{

        //Open the object pool
        PMEMobjpool *pop;
        if ((pop = pmemobj_open(file, layout)) == NULL) {
                printf("Error :File doesn't exist");
                UT_FATAL("!pmemobj_open : %s", file);
        }

        //Get the root object that we will modify
        TOID(struct test_obj) obj = (TOID(struct test_obj))pmemobj_root(pop, sizeof (struct test_obj));


        VALGRIND_EMIT_LOG("WORKLOAD.BEGIN");
        VALGRIND_EMIT_LOG("TX1.BEGIN");
  /*      TX_BEGIN(pop) {
                TX_ADD(obj);
                TX_BEGIN(pop) {
                        D_RW(obj)->a = TEST_VALUE_A;
                } TX_ONCOMMIT {
                        UT_ASSERT(D_RW(obj)->a == TEST_VALUE_A);
                        D_RW(obj)->b = TEST_VALUE_B;
                } TX_END
        } TX_ONCOMMIT {
                D_RW(obj)->c = TEST_VALUE_C;
        } TX_END
*/

	TX_BEGIN(pop) {
		TX_ADD(obj);
		D_RW(obj)->a = TEST_VALUE_A;
		D_RW(obj)->b = TEST_VALUE_B;
		D_RW(obj)->c = TEST_VALUE_C;
	} TX_END


        VALGRIND_EMIT_LOG("TX1.END");
        VALGRIND_EMIT_LOG("WORKLOAD.END");
        printf ("On commit tx values are a:%d, b%d, C:%d", D_RW(obj)->a, D_RW(obj)->b, D_RW(obj)->c);
        pmemobj_close(pop);

}

// TEST 3 : Abort the subtransaction - how does the main tx behave?
static void
do_tx_macro_abort_nested(char* file, char* layout)
{

        //Open the object pool
        PMEMobjpool *pop;
        if ((pop = pmemobj_open(file, layout)) == NULL) {
                printf("Error :File doesn't exist");
                UT_FATAL("!pmemobj_open : %s", file);
        }

        //Get the root object that we will modify
        TOID(struct test_obj) obj = (TOID(struct test_obj))pmemobj_root(pop, sizeof (struct test_obj));


        VALGRIND_EMIT_LOG("WORKLOAD.BEGIN");
        VALGRIND_EMIT_LOG("TX1.BEGIN");
        TX_BEGIN(pop) {
                TX_ADD(obj);
		D_RW(obj)->a = TEST_VALUE_A;
		D_RW(obj)->b = TEST_VALUE_B;
		D_RW(obj)->c = TEST_VALUE_C;
                TX_BEGIN(pop) {
                        D_RW(obj)->c = 1;
			pmemobj_tx_abort(EINVAL);
                } TX_END
        } TX_END

        VALGRIND_EMIT_LOG("TX1.END");
        VALGRIND_EMIT_LOG("WORKLOAD.END");
        printf ("On commit tx values are a:%d, b%d, C:%d", D_RW(obj)->a, D_RW(obj)->b, D_RW(obj)->c);
        pmemobj_close(pop);

}


// TEST 4
static void
do_tx_macro_abort_nested_main(char* file, char* layout)
{        
        //Open the object pool
        PMEMobjpool *pop;
        if ((pop = pmemobj_open(file, layout)) == NULL) {
                printf("Error :File doesn't exist");
                UT_FATAL("!pmemobj_open : %s", file);
        }
        
        //Get the root object that we will modify
        TOID(struct test_obj) obj = (TOID(struct test_obj))pmemobj_root(pop, sizeof (struct test_obj));

        
        VALGRIND_EMIT_LOG("WORKLOAD.BEGIN");
        VALGRIND_EMIT_LOG("TX1.BEGIN");
        TX_BEGIN(pop) {
                TX_ADD(obj); 
                D_RW(obj)->a = 2;
                D_RW(obj)->b = 2;
                D_RW(obj)->c = 2;
                TX_BEGIN(pop) {
			TX_ADD(obj);
                        D_RW(obj)->c = 3;
                        pmemobj_tx_commit();
                } TX_END
                pmemobj_tx_abort(EINVAL);
        } TX_END
        
        VALGRIND_EMIT_LOG("TX1.END");
        VALGRIND_EMIT_LOG("WORKLOAD.END");
        printf ("On commit tx values are a:%d, b%d, C:%d", D_RW(obj)->a, D_RW(obj)->b, D_RW(obj)->c);
        pmemobj_close(pop);

}

static int 
check_consistency(char *file, char *layout, int test_num) {
	int consistency = 0;

        char filename[100];
        sprintf(filename, "pmreorder%d.log",test_num);
        FILE *fptr;
        fptr = fopen(filename,"a");
        if(fptr == NULL)
        {
                printf("Error!");
                exit(1);
        }
	

        int result = pmemobj_check(file, layout);

        if (result < 0)
                UT_OUT("!%s: pmemobj_check", file);
        else if (result == 0)
                UT_OUT("%s: pmemobj_check: not consistent", file);

        if (result == 1)
                printf("%s: pmemobj_check: Consistent", file);

	//open the object pool and get the object
	PMEMobjpool *pop;
	if ((pop = pmemobj_open(file, layout)) == NULL)
		UT_FATAL("!pmemobj_open : %s", file);


        TOID(struct test_obj) obj = (TOID(struct test_obj))pmemobj_root(pop, sizeof (struct test_obj));

	if (D_RW(obj)->a != TEST_VALUE_A || D_RW(obj)->b != TEST_VALUE_B || D_RW(obj)->c != TEST_VALUE_C) {
		printf("\nInconsistency detected");
		printf("\na:%d, b:%d, c:%d", D_RO(obj)->a, D_RO(obj)->b, D_RO(obj)->c);	
		printf("\nShould have been a:%d, b:%d, c:%d\n", TEST_VALUE_A, TEST_VALUE_B, TEST_VALUE_C);	
	        fprintf (fptr, "\nAt this point, values are a:%d, b%d, C:%d\n", D_RW(obj)->a, D_RW(obj)->b, D_RW(obj)->c);
		consistency = 1;
	} else {

	printf("\nCOnsistent : Values -> a:%d, b:%d, c:%d", D_RO(obj)->a, D_RO(obj)->b, D_RO(obj)->c);
	}
	
	pmemobj_close(pop);

	system("cat pmemobj2.log >> out-log2.txt");
	fclose(fptr);
	return consistency;
}


static int
check_consistency_abort(char *file, char *layout, int test_num) {
        int consistency = 0;

/*	char* prefix = "../../tools/pmempool/pmempool check -rvN ";
	char* command = (char*) malloc (1 + strlen(prefix) + strlen(file));
	strcpy(command, prefix);
	strcat(command, file);
	printf("\nCommd to check = %s\n", command);
	system(command);
*/
	printf("\nFile is %s and layout is %s\n", file, layout);
	int result = pmemobj_check(file, layout);

	if (result < 0)
		UT_OUT("!%s: pmemobj_check", file);
	else if (result == 0)
		UT_OUT("%s: pmemobj_check: not consistent", file);

	if (result == 1)
		printf("%s: pmemobj_check: Consistent", file);	

	char filename[100];
	sprintf(filename, "pmreorder%d.log",test_num);
	FILE *fptr;
	fptr = fopen(filename,"a");
   	if(fptr == NULL)
   	{
     		printf("Error!");   
      		exit(1);             
   	}

        //open the object pool and get the object
        PMEMobjpool *pop;
        if ((pop = pmemobj_open(file, layout)) == NULL)
                UT_FATAL("!pmemobj_open : %s", file);


        TOID(struct test_obj) obj = (TOID(struct test_obj))pmemobj_root(pop, sizeof (struct test_obj));


        if (D_RW(obj)->a != 1 || D_RW(obj)->b != 1 || D_RW(obj)->c != 1) {
                printf("\nInconsistency detected");
                printf("\na:%d, b:%d, c:%d", D_RO(obj)->a, D_RO(obj)->b, D_RO(obj)->c);
                printf("\nShould have been a:1, b:1, c:1");
        	fprintf (fptr, "\nAt this point, values are a:%d, b%d, C:%d\n", D_RW(obj)->a, D_RW(obj)->b, D_RW(obj)->c);
                consistency = 1;
        } else {

        printf("\nCOnsistent : Values -> a:%d, b:%d, c:%d", D_RO(obj)->a, D_RO(obj)->b, D_RO(obj)->c);
        }

        pmemobj_close(pop);
	fclose(fptr);
	system("cat trace_pmemobj4.log >> out-log.txt");
        return consistency;
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_tx_flow");

	util_init();

	char* layout;
	char* file;
	int test_num = 0;

	if ((argc < 3) || (strchr("ptncabd", argv[1][0]) == NULL) || argv[1][1] != '\0')
		UT_FATAL("usage: %s p|t|n|a|c|d [file]", argv[0]);
	

	if(((argv[1][0] == 'c') || argv[1][0] == 'd') && (argc == 5)){
		test_num = strtol(argv[2], NULL, 10);	
		layout = argv[3];
		file = argv[4];
	}

	else if ((strchr("ptnab", argv[1][0]) != NULL) && (argc == 4)) {
		layout = argv[2];
		file = argv[3];
	}
	else {
		UT_FATAL("usage: %s p|t|n|a|c|d [file]", argv[0]);
	}
	printf("\n FIle is %s\n", file);
	printf("\n Layout is %s\n", layout);

	char opt = argv[1][0];

	switch (opt) {
		case 'p':
			printf("\n Creating object pool\n");
			pmreorder_create_pool(file, layout);
			break;
		case 't':
			printf("\nPerform Transaction\n");
			do_tx_macro_commit_nested(file, layout);
		
			break;
		case 'n':
			printf("\nPerform Transaction with C modified on commit\n");
			do_tx_macro_commit_nested_nopersist_c(file, layout);
			break;
		case 'a':
			printf("\nPerform Transaction abort\n");
			do_tx_macro_abort_nested(file, layout);
			break;
		case 'b':
			printf("\nPerform Transaction abort\n");
			do_tx_macro_abort_nested_main(file, layout);
			break;
		case 'c':
			// Only for checkers argv[2] is test_num and argv[3] is filename
			printf("\nPerform Consistency check\n");
			return check_consistency(file, layout, test_num); 
		case 'd':

			printf("\nPerform Consistency check for abort\n");
			return check_consistency_abort(file, layout, test_num); 
		default:
			UT_FATAL("Incorrect option %c", opt);
	}


	DONE(NULL);
}
