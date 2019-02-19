/*
 * test_concurrency.c -- unit test for undo log recovery in concurrent tx
 */
#include "unittest.h"
#include "util.h"
#include "valgrind_internal.h"
#include <string.h>
#include "sys_util.h"

#define THREADS 3
#define TEST_VALUE_A_1 5
#define TEST_VALUE_B_1 5
#define TEST_VALUE_B_2 10
#define TEST_VALUE_C_2 10
#define TEST_VALUE_C_3 15
#define TEST_VALUE_A_3 15

static os_mutex_t mtx;
static PMEMobjpool *pop;

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



static void *
change_ab(void *arg) {
        //Get the root object that we will modify
        TOID(struct test_obj) obj = (TOID(struct test_obj))pmemobj_root(pop, sizeof (struct test_obj));

        VALGRIND_EMIT_LOG("WORKLOAD.BEGIN");
        VALGRIND_EMIT_LOG("THREAD1.BEGIN");
        TX_BEGIN(pop) {
                TX_ADD(obj);
                D_RW(obj)->a = TEST_VALUE_A_1;
                D_RW(obj)->b = TEST_VALUE_B_1;
                pmemobj_tx_commit();
        } TX_END

        VALGRIND_EMIT_LOG("THREAD1.END");
        VALGRIND_EMIT_LOG("WORKLOAD.END");
        printf ("On commit tx values are a:%d, b%d, C:%d", D_RW(obj)->a, D_RW(obj)->b, D_RW(obj)->c);
	return NULL;
}

static void *
change_bc(void *arg) {
        //Get the root object that we will modify
        TOID(struct test_obj) obj = (TOID(struct test_obj))pmemobj_root(pop, sizeof (struct test_obj));

        VALGRIND_EMIT_LOG("WORKLOAD.BEGIN");
        VALGRIND_EMIT_LOG("THREAD2.BEGIN");
        TX_BEGIN(pop) {
                TX_ADD(obj); 
                D_RW(obj)->b = TEST_VALUE_B_2;
                D_RW(obj)->c = TEST_VALUE_C_2;
                pmemobj_tx_commit();
        } TX_END

        VALGRIND_EMIT_LOG("THREAD2.END");
        VALGRIND_EMIT_LOG("WORKLOAD.END");
        printf ("On commit tx values are a:%d, b%d, C:%d", D_RW(obj)->a, D_RW(obj)->b, D_RW(obj)->c);
	return NULL;
}

static void *
change_ca(void *arg) {
        //Get the root object that we will modify
        TOID(struct test_obj) obj = (TOID(struct test_obj))pmemobj_root(pop, sizeof (struct test_obj));

        VALGRIND_EMIT_LOG("WORKLOAD.BEGIN");
        VALGRIND_EMIT_LOG("THREAD3.BEGIN");
        TX_BEGIN(pop) {
                TX_ADD(obj); 
                D_RW(obj)->c = TEST_VALUE_C_3;
                D_RW(obj)->a = TEST_VALUE_A_3;
                pmemobj_tx_commit();
        } TX_END

        VALGRIND_EMIT_LOG("THREAD3.END");
        VALGRIND_EMIT_LOG("WORKLOAD.END");
        printf ("On commit tx values are a:%d, b%d, C:%d", D_RW(obj)->a, D_RW(obj)->b, D_RW(obj)->c);
	return NULL;
}



static void
do_multithread_tx(char *file, char *layout){

        //open the object pool and get the object
        if ((pop = pmemobj_open(file, layout)) == NULL)
                UT_FATAL("!pmemobj_open : %s", file);

	// create 3 threads to chnage three differnt sets of values
	int i = 0;
	os_thread_t *threads = MALLOC(THREADS * sizeof(threads[0]));
        VALGRIND_EMIT_LOG("THREADALL.BEGIN");
	PTHREAD_CREATE(&threads[i++], NULL, change_ab, NULL);
	PTHREAD_CREATE(&threads[i++], NULL, change_bc, NULL);
	PTHREAD_CREATE(&threads[i++], NULL, change_ca, NULL);

	while (i > 0)
		PTHREAD_JOIN(&threads[--i], NULL);

        VALGRIND_EMIT_LOG("THREADALL.END");
	
	FREE(threads);
	pmemobj_close(pop);
}

static int
check_consistency(char *file, char *layout, int test_num){
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

	if ((pop = pmemobj_open(file, layout)) == NULL)
                UT_FATAL("!pmemobj_open : %s", file);


        TOID(struct test_obj) obj = (TOID(struct test_obj))pmemobj_root(pop, sizeof (struct test_obj));

        if ((D_RW(obj)->a != TEST_VALUE_A_1) && (D_RW(obj)->a != TEST_VALUE_A_3)){
                printf("\nInconsistency detected");
                printf("\na:%d, b:%d, c:%d", D_RO(obj)->a, D_RO(obj)->b, D_RO(obj)->c);
                printf("\nShould have been a:%d or %d\n", TEST_VALUE_A_1, TEST_VALUE_A_3);
                fprintf (fptr, "\nAt this point, values are a:%d, b%d, C:%d\n", D_RW(obj)->a, D_RW(obj)->b, D_RW(obj)->c);
                consistency = 1;
	}
        if ((D_RW(obj)->b != TEST_VALUE_B_1) && (D_RW(obj)->b != TEST_VALUE_B_2)){
                printf("\nInconsistency detected");
                printf("\na:%d, b:%d, c:%d", D_RO(obj)->a, D_RO(obj)->b, D_RO(obj)->c);
                printf("\nShould have been b:%d or %d\n", TEST_VALUE_B_1, TEST_VALUE_B_2);
                fprintf (fptr, "\nAt this point, values are a:%d, b%d, C:%d\n", D_RW(obj)->a, D_RW(obj)->b, D_RW(obj)->c);
                consistency = 1;
	}
        if ((D_RW(obj)->c != TEST_VALUE_C_2) && (D_RW(obj)->c != TEST_VALUE_C_3)){
                printf("\nInconsistency detected");
                printf("\na:%d, b:%d, c:%d", D_RO(obj)->a, D_RO(obj)->b, D_RO(obj)->c);
                printf("\nShould have been c:%d or %d\n", TEST_VALUE_C_2, TEST_VALUE_C_3);
                fprintf (fptr, "\nAt this point, values are a:%d, b%d, C:%d\n", D_RW(obj)->a, D_RW(obj)->b, D_RW(obj)->c);
                consistency = 1;
	}
	if (consistency == 0){
                fprintf (fptr, "\nAt this point, values are a:%d, b%d, C:%d\n", D_RW(obj)->a, D_RW(obj)->b, D_RW(obj)->c);
		printf("\nCONSISTENT : Values -> a:%d, b:%d, c:%d\n", D_RO(obj)->a, D_RO(obj)->b, D_RO(obj)->c);
	}

	pmemobj_close(pop);
	system("cat pmemobj1.log >> out-log1.txt");
	fclose(fptr);
	return consistency;
}


int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_multithread");

	util_init();

	util_mutex_init(&mtx);

	char* layout;
	char* file;
	int test_num = 0;

	if ((argc < 3) || (strchr("pmc", argv[1][0]) == NULL) || argv[1][1] != '\0')
		UT_FATAL("usage: %s m|c [file]", argv[0]);
	

	if(((argv[1][0] == 'c') || argv[1][0] == 'd') && (argc == 5)){
		test_num = strtol(argv[2], NULL, 10);	
		layout = argv[3];
		file = argv[4];
	}

	else if ((strchr("pm", argv[1][0]) != NULL) && (argc == 4)) {
		layout = argv[2];
		file = argv[3];
	}
	else {
		UT_FATAL("usage: %s p|t|n|a|c|d [file]", argv[0]);
	}
	printf("\n File is %s\n", file);
	printf("\n Layout is %s\n", layout);

	char opt = argv[1][0];

	switch (opt) {
		case 'p':
			printf("\n Creating object pool\n");
			pmreorder_create_pool(file, layout);
			break;
		case 'm':
			printf("\nPerform Multithreaded Transaction\n");
			do_multithread_tx(file, layout);
			break;
		case 'c':
			// Only for checkers argv[2] is test_num and argv[3] is filename
			printf("\nPerform Consistency check\n");
			return check_consistency(file, layout, test_num); 
		default:
			UT_FATAL("Incorrect option %c", opt);
	}

	util_mutex_destroy(&mtx);
	DONE(NULL);
}
