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
    def lineage(self) -> List[ATuple]:
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        temp=[[ATuple(self.tuple,self.metadata,self.operator)]]
        return self.operator.lineage(temp)
        pass

    # Returns the Where-provenance of the attribute at index 'att_index' of self
    def where(att_index) -> List[Tuple]:
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        return self.operator.where(att_index,self)
        pass

    # Returns the How-provenance of self
    def how(self) -> string:
        # YOUR CODE HERE (ONLY FOR TASK 3 IN ASSIGNMENT 2)
        return self.metadata
        pass

    # Returns the input tuples with responsibility \rho >= 0.5 (if any)
    def responsible_inputs() -> List[Tuple]:
        # YOUR CODE HERE (ONLY FOR TASK 4 IN ASSIGNMENT 2)
        pass
    #def __eq__(self, value):
     #   return self.tuple==value.tuple and self.metadata==value.metadata and self.operator==value.operator

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
        super(Scan, self).__init__(name="Scan", track_prov=track_prov,
                                   propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.filepath=filepath
        self.filter=filter
        #self.track_prov=track_prov
        #self.propagate_prov=propagate_prov
        f=open(self.filepath)
        self.f_csv = csv.reader(f)
        self.intermediate={}
        self.linenum=0
        pass

    # Returns next batch of tuples in given file (or None if file exhausted)
    def get_next(self):
        # YOUR CODE HERE
        AT_list=[]
        try:
            while len(AT_list)<5:
                headers = next(self.f_csv)
                self.linenum+=1;
                tuple1=",".join(headers)
                if self.filter:
                    if not self.filter.apply(tuple1):
                        continue
                if self.propagate_prov:
                    if self.filepath=="../data/friends.txt":
                        atuple1=ATuple(tuple1,"f"+str(self.linenum),self)
                    else:
                        atuple1=ATuple(tuple1,"r"+str(self.linenum),self)
                else:
                    atuple1=ATuple(tuple1,None,self)
                if self.track_prov:
                    self.intermediate[tuple1]=self.linenum
                AT_list.append(atuple1)
        except StopIteration:
            return AT_list
        return AT_list
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        return tuples
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        ans_list=[]
        for i in tuples:
            tuple1=i.tuple
            tuple1_list=tuple1.split(" ")
            att_value=tuple1_list[att_index]
            ans_tuple=(self.filepath,self.intermediate[tuple1],tuple1,att_value)
            ans_list.append(ans_tuple)
        return ans_list
        pass

class Filter:
    def __init__(self, user_id, index): 
        self.uid = user_id
        self.att = index
    def apply(self, tuple1):
        tuple_list=tuple1.split(" ")
        return tuple_list[self.att] == self.uid 

# Equi-join operator
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
        super(Join, self).__init__(name="Join", track_prov=track_prov,
                                   propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.left_input=left_input
        self.right_input=right_input
        self.left_join_attribute=left_join_attribute
        self.right_join_attribute=right_join_attribute
        self.dict1={}
        self.intermediate={}
        left_result="0"
        i=1
        while(len(left_result)==5 or i==1):
            i=0
            left_result=left_input.get_next()
            for i in left_result:
                num_list=i.tuple.split(' ')
                try:
                    self.dict1[num_list[left_join_attribute]].append(i)
                except KeyError:
                    self.dict1[num_list[left_join_attribute]]=[i]
        pass

    # Returns next batch of joined tuples (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        result=[]
        tuple1=self.right_input.get_next()
        while len(tuple1)==5:
            for i in tuple1:
                num_list2=i.tuple.split(' ')
                if num_list2[self.right_join_attribute] in self.dict1:
                    for j in self.dict1[num_list2[self.right_join_attribute]]:
                        str_tuple=j.tuple+' '+i.tuple
                        if self.propagate_prov:
                            atuple1=ATuple(str_tuple,j.metadata+"*"+i.metadata,self)
                        else:
                            atuple1=ATuple(str_tuple,None,self)
                        result.append(atuple1)
                        if self.track_prov:
                            self.intermediate[atuple1.tuple]=[j,i]
            tuple1=self.right_input.get_next()
        return result
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        answer_list=[]
        for i in tuples:
            left_list=[]
            right_list=[]
            for j in i:
                left_tuple=self.intermediate[j.tuple][0]
                left_list.append(self.left_input.lineage(left_tuple))
                right_tuple=self.intermediate[j.tuple][1]
                right_list.append(self.right_input.lineage(right_tuple))
            answer_list.append(left_list+right_list)
        return answer_list
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        ans_tuple=[]
        op=0
        for i in tuples:
            if att_index>1:
                ans_tuple.append(self.intermediate[i.tuple][1])
                op=self.right_input
            else:
                ans_tuple.append(self.intermediate[i.tuple][0])
                op=self.left_input
        if att_index<2:
            return op.where(att_index,ans_tuple)
        else:
            return op.where(att_index - 2,ans_tuple)
        pass

# Project operator
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
        super(Project, self).__init__(name="Project", track_prov=track_prov,
                                      propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input=input
        self.fields_to_keep=fields_to_keep
        self.intermediate={}
        pass

    # Return next batch of projected tuples (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        tuple_list=self.input.get_next()
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
            if self.propagate_prov:
                result=ATuple(str1[0:-1],i.metadata,self)
            else:
                result=ATuple(str1[0:-1],None,self)
            return_list.append(result)
            if self.track_prov:
                self.intermediate[result.tuple]=i
        return return_list

        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)

        if not self.fields_to_keep:
            return self.input.lineage(tuples) 
        else:
            ans_list=[]
            for i in tuples:
                ans_part_list=[]
                for j in i:
                    ans_part_list.append(self.intermediate[j.tuple])
                ans_list.append(ans_part_list)

            return self.input.lineage(ans_list)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        ans_list=[]
        for i in tuples:
            ans_list.append(self.intermediate[i.tuple])
        return self.input.where(self.fields_to_keep[att_index],ans_list)
        pass

# Group-by operator
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
        super(GroupBy, self).__init__(name="GroupBy", track_prov=track_prov,
                                      propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input=input
        self.key=key
        self.value=value
        self.agg_fun=agg_fun
        self.intermediate={}
        pass

    # Returns aggregated value per distinct key in the input (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        input_list={}
        output_list=[]
        while True :
            input1=self.input.get_next()
            if not input1:
                break
            for i in input1:
                tuple_list=i.tuple.split(" ")
                try:
                    input_list[tuple_list[self.key]].append(i)
                except KeyError:
                    input_list[tuple_list[self.key]]=[i]
        for j in input_list:
            real_tuple_list=input_list[j][0].tuple.split(" ")
            tuple1=real_tuple_list[self.key]+" "+str(self.agg_fun.AVG(input_list[j],self.value))
            if self.propagate_prov:
                metadata_str=""
                for k in input_list[j]:
                    k_list=k.tuple.split(" ")
                    metadata_str=metadata_str+" ("+k.metadata+"@"+k_list[self.value]+"),"

                atuple1=ATuple(tuple1,"AVG("+metadata_str[:-1]+" )",self)
            else:
                atuple1=ATuple(tuple1,None,self)
            output_list.append(atuple1)
            if self.track_prov:
                self.intermediate[atuple1.tuple]=input_list[j]
        return output_list
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        ans_list=[]
        for i in tuples:
            for j in i:
                ans_list.append(self.intermediate[j.tuple])
        return self.input.lineage(ans_list)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        ans_list=[]
        for i in tuples:
            ans_list.extend(self.intermediate[i.tuple])
        return self.input.where(self.value,ans_list)
        pass

class AggFun:
    def __init__(self): 
        pass

    def AVG(self, list, index):
        sum=0.0
        for i in list:
            tuple_list=i.tuple.split(" ")
            sum+=int(tuple_list[index])
        length=len(list)
        return sum/length 


# Custom histogram operator
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
        super(Histogram, self).__init__(name="Histogram",
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
            input1=self.input.get_next()
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
        super(OrderBy, self).__init__(name="OrderBy",
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
            input1=self.input.get_next()
            if not input1:
                break
            for i in input1:
                if self.propagate_prov:
                    output_list.append(ATuple(i.tuple,i.metadata,self))
                else:
                    output_list.append(ATuple(i.tuple,None,self))
        return self.comparator.apply(output_list,self.ASC)
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        return self.input.lineage(tuples)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        return self.input.where(att_index,tuples)
        pass

class Comparator:
    def __init__(self,att): 
        self.att=att

    def apply(self, list, ASC):
        list.sort(key=lambda x:x.tuple.split()[self.att],reverse=not ASC)
        return list 

# Top-k operator
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
        super(TopK, self).__init__(name="TopK", track_prov=track_prov,
                                   propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input=input
        self.k=k
        pass

    # Returns the first k tuples in the input (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        input1=self.input.get_next()
        if self.k==None:
            return input1[0]
        output_list=[]
        for i in range(0,self.k):
            if self.propagate_prov:
                output_list.append(ATuple(input1[i].tuple,input1[i].metadata,self))
            else:
                output_list.append(ATuple(input1[i].tuple,None,self))
        return output_list
        pass

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        return self.input.lineage(tuples)
        pass

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        return self.input.where(att_index,tuples)
        pass

# Filter operator
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
        super(Filter, self).__init__(name="Select", track_prov=track_prov,
                                     propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input=input
        self.predicate=predicate
        pass

    # Returns next batch of tuples that pass the filter (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        output_list=[]
        input1=self.input.get_next()
        for i in input1:
            if(self.predicate.apply(i)):
                output_list.append(i)
        return output_list
        pass

class Predicate:
	# user_id is the user-provided value in the above command
    def __init__(self, user_id, index): 
        self.uid = user_id
        self.att = index

  # Returns true if tuple passes the filter, false otherwise
    def apply(self, tuple):
        return tuple[self.att] == self.uid 

class Distinct(Operator):
    def __init__(self, input, fields_to_compare=0, track_prov=False,
                                                    propagate_prov=False):
        self.input=input
        self.fields_to_compare=fields_to_compare

    def get_next(self):
        ans={}
        while True:
            input_tuple=self.input.get_next()
            if not input_tuple:
                break
            for i in input_tuple:
                #logger.info(i.tuple)
                input_tuple_list=i.tuple.split(",")
                #logger.info(input_tuple_list)
                key=input_tuple_list[self.fields_to_compare]
                if key=="":
                    key="empty"
                if key=="Mozilla" or key=="Mozilla Firefox":
                    key="Firefox"
                if key=="Internet Explorer" or key == "InternetExplorer":
                    key="IE"
                if key=="Google Chrome":
                    key="Chrome"
                if(key in ans):
                    ans[key]+=1
                else:
                    ans[key]=1
        return ans

class Map(Operator):
    def __init__(self, input, keys, track_prov=False, propagate_prov=False):
        self.input=input
        self.keys=keys
    def get_next(self):
        ans=[]
        while True:
            tuple_list=self.input.get_next()
            if not tuple_list:
                break
            for i in tuple_list:
                att_list=i.tuple.split(",")
                for j in range(0,len(att_list)):
                    if not att_list[j]:
                        att_list[j]="0"
                    elif j==1:
                        time_list=att_list[j].split(" ")
                        date_list=time_list[0].split("-")
                        hour_list=time_list[1].split(":")
                        string_wanted="-".join(date_list[1:3])+" "+":".join(hour_list[0:2])
                        att_list[j]=string_wanted
                    elif j in [6,7,8]:
                        name_string=att_list[j]
                        if name_string=="Mozilla" or name_string=="Mozilla Firefox":
                            name_string="Firefox"
                        if name_string=="Internet Explorer" or name_string == "InternetExplorer":
                            name_string="IE"
                        if name_string=="Google Chrome":
                            name_string="Chrome"
                        att_list[j]=self.keys[name_string]
                i=",".join(att_list[1:10])
                ans.append(i)
        return ans;


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
    if sys.argv[2]=="1":
        filter_friend=Filter(sys.argv[8],0)
        scan_friends=Scan(sys.argv[4],filter_friend,True,False)
        filter_movie=Filter(sys.argv[10],1)
        scan_movies=Scan(sys.argv[6],filter_movie,True,False)
        join_opt=Join(scan_friends,scan_movies,1,0,True,False)
        average=AggFun()
        #projection=Project(join_opt,[4],False,False)
        tuples=join_opt.get_next()
        ans1=average.AVG(tuples,4)
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
    if sys.argv[2]=="2":
        filter_friend2=Filter(sys.argv[8],0)
        scan_friends2=Scan(sys.argv[4],filter_friend2,True,True)
        scan_movies2=Scan(sys.argv[6],None,True,True)
        join_opt2=Join(scan_friends2,scan_movies2,1,0,True,True)
        #projection_pre2=Project(join_opt2,[3,4],True,True)
        average2=AggFun()
        group_by_opt2=GroupBy(join_opt2,3,4,average2,True,True)
        compare_opt2=Comparator(1)
        order_by_opt2=OrderBy(group_by_opt2,compare_opt2,False,True,True)
        top_k_opt2=TopK(order_by_opt2,1,True,True)
        projection2=Project(top_k_opt2,[0],True,True)
        ans2=projection2.get_next()[0]
        logger.info(int(ans2.tuple))

    # TASK 3: Implement explanation query for User A and Movie M
    #
    # SELECT HIST(R.Rating) as explanation
    # FROM Friends as F, Ratings as R
    # WHERE F.UID2 = R.UID
    #       AND F.UID1 = 'A'
    #       AND R.MID = 'M'

    # YOUR CODE HERE
    if sys.argv[2]=="3":
        filter_friend=Filter(sys.argv[8],0)
        scan_friends=Scan(sys.argv[4],filter_friend,False,False)
        filter_movie=Filter(sys.argv[10],1)
        scan_movies=Scan(sys.argv[6],filter_movie,False,False)
        join_opt=Join(scan_friends,scan_movies,1,0,False,False)
        #average=AggFun()
        #projection=Project(join_opt,[4],False,False)
        hist_opt=Histogram(join_opt,4,False,False)
        ans3=hist_opt.get_next()
        logger.info(ans3)

    # TASK 4: Turn your data operators into Ray actors
    #
    # NOTE (john): Add your changes for Task 4 to a new git branch 'ray'


    logger.info("Assignment #2")

    # TASK 1: Implement lineage query for movie recommendation

    # YOUR CODE HERE
    if sys.argv[2]=="2":
        ans2_lineage_list=ans2.lineage()
        ans_tuple_list=[]
        if(len(ans2_lineage_list)==1):
            for i in ans2_lineage_list:
                for j in i:
                    ans_tuple_list.append(j.tuple)
        else:
            part=[]
            for i in ans2_lineage_list:
                for j in i:
                    part.append(j.tuple)
                ans_tuple_list.append(part)
        logger.info(ans_tuple_list)

    # TASK 2: Implement where-provenance query for 'likeness' prediction

    # YOUR CODE HERE
    if sys.argv[2]=="1":
        as2_tsk2=join_opt.where(4,tuples)
        logger.info(as2_tsk2)


    # TASK 3: Implement how-provenance query for movie recommendation

    # YOUR CODE HERE
    if sys.argv[2]=="2":
        """ 
        another way
        half_len=int(len(ans_tuple_list)/2)
        as2_tsk3=""
        for i in range(half_len):
            one_pair="(f"+str(scan_friends2.intermediate[ans_tuple_list[i]])+"*r"+str(scan_movies2.intermediate[ans_tuple_list[i+half_len]])+"@"+ans_tuple_list[i+half_len].split(" ")[2]+"), "
            as2_tsk3=as2_tsk3+one_pair
        as2_tsk3="AVG( "+as2_tsk3[0:len(as2_tsk3)-2]+" )"
        logger.info(as2_tsk3)
        """
        logger.info(ans2.how())
    # TASK 4: Retrieve most responsible tuples for movie recommendation

    # YOUR CODE HERE
    if sys.argv[2]=="2":
        all_how=group_by_opt2.intermediate
        #request=all_how[ans2.tuple]
        ans_key=projection2.intermediate[ans2.tuple].tuple
        max=ans_key.split(" ")[1]
        max_other=0
        if len(all_how[ans_key])==1:
            responsibility_list=all_how[ans_key][0].tuple.split(" ")
            logger.info( [((responsibility_list[0],responsibility_list[1]),1),((responsibility_list[2],responsibility_list[3],responsibility_list[4]),1)])
        elif len(all_how[ans_key])==2:
            t1=all_how[ans_key][0].tuple.split(" ")
            t2=all_how[ans_key][1].tuple.split(" ")
            if(t1[4]>max):
                logger.info( [((t1[0],t1[1]),1),((t1[2],t1[3],t1[4]),1)])
            if(t2[4]>max):
                logger.info( [((t2[0],t2[1]),1),((t2[2],t2[3],t2[4]),1)])
            else:
                logger.info( [((t1[0],t1[1]),0.5),((t1[2],t1[3],t1[4]),0.5),((t2[0],t2[1]),0.5),((t2[2],t2[3],t2[4]),0.5)])
        else:
            ans_list=[]
            for i in all_how:
                temp_score=i.split(" ")[1]
                if max_other<temp_score:
                    max_other=temp_score
            for i in all_how[ans_key]:
                test_for_counterfactual=(len(all_how[ans_key])*max-i.tuple.split(" ")[4])/(len(all_how[ans_key])-1)
                if test_for_counterfactual<max_other:
                    temp=i.tuple.split(" ")
                    ans_list.append(((temp[0],temp[1]),1))
                    ans_list.append(((temp[2],temp[3],temp[4]),1))
            for i in all_how[ans_key]:
                for j in all_how[ans_key]:
                    if i!=j:
                        test_for_05=(len(all_how[ans_key])*max-i.tuple.split(" ")[4]-j.tuple.split(" ")[4])/(len(all_how[ans_key])-2)
                        if test_for_05<max_other:
                            temp=i.tuple.split(" ")
                            ans_list.append(((temp[0],temp[1]),0.5))
                            ans_list.append(((temp[2],temp[3],temp[4]),0.5))
                            break
            logger.info(ans_list)

