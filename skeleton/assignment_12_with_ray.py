
from __future__ import absolute_import
from __future__ import annotations
from __future__ import division
from __future__ import print_function

import csv
import logging
from typing import List, Tuple
import uuid
import sys

import ray
ray.init()

# Note (john): Make sure you use Python's logger to log
#              information about your program
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

# Generates unique operator IDs
def _generate_uuid():
    return uuid.uuid4()


# Custom tuple class with optional metadata
class ATuple:
    """Custom tuple.

    Attributes:
        tuple (Tuple): The actual tuple.
        metadata (string): The tuple metadata (e.g. provenance annotations).
        operator (Operator): A handle to the operator that produced the tuple.
    """
    def __init__(self, tuple, metadata=None, operator=None):
        self.tuple = tuple
        self.metadata = metadata
        self.operator = operator

    # Returns the lineage of self
    def lineage() -> List[ATuple]:
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the Where-provenance of the attribute at index 'att_index' of self
    def where(att_index) -> List[Tuple]:
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

    # Returns the How-provenance of self
    def how() -> string:
        # YOUR CODE HERE (ONLY FOR TASK 3 IN ASSIGNMENT 2)
        pass

    # Returns the input tuples with responsibility \rho >= 0.5 (if any)
    def responsible_inputs() -> List[Tuple]:
        # YOUR CODE HERE (ONLY FOR TASK 4 IN ASSIGNMENT 2)
        pass

# Data operator
class Operator:
    """Data operator (parent class).

    Attributes:
        id (string): Unique operator ID.
        name (string): Operator name.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    def __init__(self, id=None, name=None, track_prov=False,
                                           propagate_prov=False):
        self.id = _generate_uuid() if id is None else id
        self.name = "Undefined" if name is None else name
        self.track_prov = track_prov
        self.propagate_prov = propagate_prov
        logger.debug("Created {} operator with id {}".format(self.name,
                                                             self.id))

    # NOTE (john): Must be implemented by the subclasses
    def get_next(self):
        logger.error("Method not implemented!")

    # NOTE (john): Must be implemented by the subclasses
    def lineage(self, tuples: List[ATuple]) -> List[List[ATuple]]:
        logger.error("Lineage method not implemented!")

    # NOTE (john): Must be implemented by the subclasses
    def where(self, att_index: int, tuples: List[ATuple]) -> List[List[Tuple]]:
        logger.error("Where-provenance method not implemented!")

# Scan operator
@ray.remote
class Scan(Operator):
    """Scan operator.

    Attributes:
        filepath (string): The path to the input file.
        filter (function): An optional user-defined filter.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes scan operator
    def __init__(self, filepath, filter=None, track_prov=False,
                                              propagate_prov=False):
        Operator.__init__(self,name="Scan", track_prov=track_prov,
                                   propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.filepath=filepath
        self.filter=filter
        #self.track_prov=track_prov
        #self.propagate_prov=propagate_prov
        f=open(self.filepath)
        self.f_csv = csv.reader(f)
        pass

    # Returns next batch of tuples in given file (or None if file exhausted)
    def get_next(self):
        # YOUR CODE HERE
        AT_list=[]
        try:
            while len(AT_list)<5:
                headers = next(self.f_csv)
                tuple1=headers[0]
                if self.filter:
                    if not self.filter.apply.remote(tuple1):
                        continue
                atuple=ATuple(tuple1,None,Scan)
                AT_list.append(atuple)
        except StopIteration:
            return AT_list
        return AT_list
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

@ray.remote
class Filter:
    def __init__(self, user_id, index): 
        self.uid = user_id
        self.att = index
    def apply(self, tuple1):
        tuple_list=tuple1.split(" ")
        return tuple_list[self.att] == self.uid 

# Equi-join operator
@ray.remote
class Join(Operator):
    """Equi-join operator.

    Attributes:
        left_input (Operator): A handle to the left input.
        right_input (Operator): A handle to the left input.
        left_join_attribute (int): The index of the left join attribute.
        right_join_attribute (int): The index of the right join attribute.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes join operator
    def __init__(self, left_input, right_input, left_join_attribute,
                                                right_join_attribute,
                                                track_prov=False,
                                                propagate_prov=False):
        #super(Join.init, self)
        Operator.__init__(self,name="Join", track_prov=track_prov,
                                   propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.left_input=left_input
        self.right_input=right_input
        self.left_join_attribute=left_join_attribute
        self.right_join_attribute=right_join_attribute
        self.dict1={}
        left_result="0"
        i=1
        while(len(left_result)==5 or i==1):
            i=0
            left_future=left_input.get_next.remote();
            left_result=ray.get(left_future)#left_input.get_next()
            #left_result=left_input.get_next()
            for i in left_result:
                #logger.debug(i.tuple)
                num_list=i.tuple.split(" ")
                try:
                    self.dict1[num_list[left_join_attribute]].append(i)
                except KeyError:
                    self.dict1[num_list[left_join_attribute]]=[i]
        pass

    # Returns next batch of joined tuples (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        result=[]
        right_future=self.right_input.get_next.remote();
        tuple1=ray.get(right_future)#self.right_input.get_next()
        while len(tuple1)==5:
            for i in tuple1:
                num_list2=i.tuple.split(" ")
                if num_list2[self.right_join_attribute] in self.dict1:
                    for j in self.dict1[num_list2[self.right_join_attribute]]:
                        str_tuple=(j.tuple+" "+i.tuple)
                        atuple1=ATuple(str_tuple,None,Join)
                        result.append(atuple1)
            right_future=self.right_input.get_next.remote();
            tuple1=ray.get(right_future)#self.right_input.get_next()
        return result
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

# Project operator
@ray.remote
class Project(Operator):
    """Project operator.

    Attributes:
        input (Operator): A handle to the input.
        fields_to_keep (List(int)): A list of attribute indices to keep.
        If empty, the project operator behaves like an identity map, i.e., it
        produces and output that is identical to its input.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes project operator
    def __init__(self, input, fields_to_keep=[], track_prov=False,
                                                 propagate_prov=False):
        Operator.__init__(self,name="Project", track_prov=track_prov,
                                      propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input=input
        self.fields_to_keep=fields_to_keep
        pass

    # Return next batch of projected tuples (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        future=self.input.get_next.remote();
        tuple_list=ray.get(future)#self.input.get_next()
        return_list=[]
        if not self.fields_to_keep:
            return tuple_list
        for i in tuple_list:
            origin_tuple=i.tuple
            att_list=origin_tuple.split(" ")
            str1=""
            for j in self.fields_to_keep:
                str1+=att_list[j]
                str1+=" "
            return_list.append(ATuple(str1[0:-1],None,Project))
        return return_list

        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

# Group-by operator
@ray.remote
class GroupBy(Operator):
    """Group-by operator.

    Attributes:
        input (Operator): A handle to the input
        key (int): The index of the key to group tuples.
        value (int): The index of the attribute we want to aggregate.
        agg_fun (function): The aggregation function (e.g. AVG)
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes average operator
    def __init__(self, input, key, value, agg_fun, track_prov=False,
                                                   propagate_prov=False):
        Operator.__init__(self,name="GroupBy", track_prov=track_prov,
                                      propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input=input
        self.key=key
        self.value=value
        self.agg_fun=agg_fun

        pass

    # Returns aggregated value per distinct key in the input (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        input_list={}
        output_list=[]
        while True :
            future=self.input.get_next.remote();
            input1=ray.get(future)#self.input.get_next()
            if not input1:
                break
            for i in input1:
                tuple_list=i.tuple.split(" ")
                try:
                    input_list[tuple_list[self.key]].append(i)
                except KeyError:
                    input_list[tuple_list[self.key]]=[i]
        for j in input_list:
            #logger.debug(input_list[j][0].tuple)
            real_tuple_list=input_list[j][0].tuple.split(" ")
            tuple1=real_tuple_list[self.key]+" "+str(self.agg_fun.AVG(input_list[j],self.value))
            atuple1=ATuple(tuple1,None,GroupBy)
            output_list.append(atuple1)
        return output_list
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

@ray.remote
class AggFun:
    def __init__(self): 
        pass

    def AVG(self, list, index):
        #logger.debug(list)
        sum=0.0
        for i in list:
            tuple_list=i.tuple.split(" ")
            sum+=int(tuple_list[index])
        length=len(list)
        return sum/length 


# Custom histogram operator
@ray.remote
class Histogram(Operator):
    """Histogram operator.

    Attributes:
        input (Operator): A handle to the input
        key (int): The index of the key to group tuples. The operator outputs
        the total number of tuples per distinct key.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes histogram operator
    def __init__(self, input, key=0, track_prov=False, propagate_prov=False):
        Operator.__init__(self,name="Histogram",
                                        track_prov=track_prov,
                                        propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input=input
        self.key=key
        pass

    # Returns histogram (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        output_list={}
        while True:
            future=self.input.get_next.remote();
            input1=ray.get(future)#self.input.get_next()
            if not input1:
                break
            for i in input1:
                tuple_list=i.tuple.split(" ")
                try:
                    output_list[tuple_list[self.key]+" points"].append(i.tuple)
                except KeyError:
                    output_list[tuple_list[self.key]+" points"]=[i.tuple]
        for i in output_list:
            output_list[i]=len(output_list[i])
        return output_list
        pass

# Order by operator
@ray.remote
class OrderBy(Operator):
    """OrderBy operator.

    Attributes:
        input (Operator): A handle to the input
        comparator (function): The user-defined comparator used for sorting the
        input tuples.
        ASC (bool): True if sorting in ascending order, False otherwise.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes order-by operator
    def __init__(self, input, comparator, ASC=True, track_prov=False,
                                                    propagate_prov=False):
        Operator.__init__(self,name="OrderBy",
                                      track_prov=track_prov,
                                      propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input=input
        self.comparator=comparator
        self.ASC=ASC
        pass

    # Returns the sorted input (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        output_list=[]
        while True:
            future=self.input.get_next.remote();
            input1=ray.get(future)#self.input.get_next()
            if not input1:
                break
            for i in input1:
                output_list.append(ATuple(i.tuple,None,OrderBy))
        return self.comparator.apply(output_list,self.ASC)
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

@ray.remote
class Comparator:
    def __init__(self,att): 
        self.att=att

    def apply(self, list, ASC):
        list.sort(key=lambda x:x.tuple.split()[self.att],reverse=not ASC)
        return list 

# Top-k operator
@ray.remote
class TopK(Operator):
    """TopK operator.

    Attributes:
        input (Operator): A handle to the input.
        k (int): The maximum number of tuples to output.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes top-k operator
    def __init__(self, input, k=None, track_prov=False, propagate_prov=False):
        Operator.__init__(self,name="TopK", track_prov=track_prov,
                                   propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input=input
        self.k=k
        pass

    # Returns the first k tuples in the input (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        future=self.input.get_next.remote();
        input1=ray.get(future)#self.input.get_next()
        if self.k==None:
            return input1[0]
        output_list=[]
        for i in range(0,self.k):
            output_list.append(input1[i])
        return output_list
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        pass

# Filter operator
@ray.remote
class Select(Operator):
    """Select operator.

    Attributes:
        input (Operator): A handle to the input.
        predicate (function): The selection predicate.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes select operator
    def __init__(self, input, predicate, track_prov=False,
                                         propagate_prov=False):
        Operator.__init__(self,name="Select", track_prov=track_prov,
                                     propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input=input
        self.predicate=predicate
        pass

    # Returns next batch of tuples that pass the filter (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        output_list=[]
        future=self.input.get_next.remote();
        input1=ray.get(future)#self.input.get_next()
        for i in input1:
            if(self.predicate.apply(i)):
                output_list.append(i)
        return output_list
        pass

@ray.remote
class Predicate:
	# user_id is the user-provided value in the above command
    def __init__(self, user_id, index): 
        self.uid = user_id
        self.att = index

  # Returns true if tuple passes the filter, false otherwise
    def apply(self, tuple):
        return tuple[self.att] == self.uid 

if __name__ == "__main__":

    logger.info("Assignment #1")

    # TASK 1: Implement 'likeness' prediction query for User A and Movie M
    #
    # SELECT AVG(R.Rating)
    # FROM Friends as F, Ratings as R
    # WHERE F.UID2 = R.UID
    #       AND F.UID1 = 'A'
    #       AND R.MID = 'M'

    # YOUR CODE HERE
    filter_friend=Filter.remote(sys.argv[2],0)
    scan_friends=Scan.remote("../data/friends.txt",filter_friend,False,False)
    filter_movie=Filter.remote(sys.argv[4],1)
    scan_movies=Scan.remote("../data/movie_ratings.txt",filter_movie,False,False)
    join_opt=Join.remote(scan_friends,scan_movies,1,0,False,False)
    average=AggFun.remote()
    #projection=Project.remote(join_opt,[4],False,False)
    join_future=join_opt.get_next.remote()
    ans1=ray.get(average.AVG.remote(join_future,4))
    logger.info(ans1)

    # TASK 2: Implement recommendation query for User A
    #
    # SELECT R.MID
    # FROM ( SELECT R.MID, AVG(R.Rating) as score
    #        FROM Friends as F, Ratings as R
    #        WHERE F.UID2 = R.UID
    #              AND F.UID1 = 'A'
    #        GROUP BY R.MID
    #        ORDER BY score DESC
    #        LIMIT 1 )

    # YOUR CODE HERE
    filter_friend=Filter.remote(sys.argv[2],0)
    scan_friends=Scan.remote("../data/friends.txt",filter_friend,False,False)
    scan_movies=Scan.remote("../data/movie_ratings.txt",None,False,False)
    join_opt=Join.remote(scan_friends,scan_movies,1,0,False,False)
    projection_pre=Project.remote(join_opt,[3,4],False,False)
    average=AggFun.remote()
    group_by_opt=GroupBy.remote(projection_pre,0,1,average,False,False)
    compare_opt=Comparator.remote(1)
    order_by_opt=OrderBy.remote(group_by_opt,compare_opt,False,False,False)
    top_k_opt=TopK.remote(order_by_opt,1,False,False)
    projection=Project.remote(top_k_opt,[0],False,False)
    ans2=ray.get(projection.get_next.remote())[0]
    logger.info(int(ans2.tuple))

    # TASK 3: Implement explanation query for User A and Movie M
    #
    # SELECT HIST(R.Rating) as explanation
    # FROM Friends as F, Ratings as R
    # WHERE F.UID2 = R.UID
    #       AND F.UID1 = 'A'
    #       AND R.MID = 'M'

    # YOUR CODE HERE
    filter_friend=Filter.remote(sys.argv[2],0)
    scan_friends=Scan.remote("../data/friends.txt",filter_friend,False,False)
    filter_movie=Filter.remote(sys.argv[4],1)
    scan_movies=Scan.remote("../data/movie_ratings.txt",filter_movie,False,False)
    join_opt=Join.remote(scan_friends,scan_movies,1,0,False,False)
    #average=AggFun.remote()
    #projection=Project.remote(join_opt,[4],False,False)
    hist_opt=Histogram.remote(join_opt,4,False,False)
    ans3=ray.get(hist_opt.get_next.remote())
    logger.info(ans3)

    # TASK 4: Turn your data operators into Ray actors
    #
    # NOTE (john): Add your changes for Task 4 to a new git branch 'ray'


    logger.info("Assignment #2")

    # TASK 1: Implement lineage query for movie recommendation

    # YOUR CODE HERE


    # TASK 2: Implement where-provenance query for 'likeness' prediction

    # YOUR CODE HERE


    # TASK 3: Implement how-provenance query for movie recommendation

    # YOUR CODE HERE


    # TASK 4: Retrieve most responsible tuples for movie recommendation

    # YOUR CODE HERE
