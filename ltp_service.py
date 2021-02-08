# -*- coding: utf-8 -*-
import sys, os, logging
from triple_extraction import TripleExtractor
from ltp_parser import LtpParser

class LtpService:
    def __init__(self):
        self.logger = logging.getLogger(__name__)


    def get_entities(self, seq, suffix=False):
        """Gets entities from sequence.

        Args:
            seq (list): sequence of labels.

        Returns:
            list: list of (chunk_type, chunk_start, chunk_end).

        Example:
            from seqeval.metrics.sequence_labeling import get_entities
            seq = ['B-PER', 'I-PER', 'O', 'B-LOC']
            get_entities(seq)
            [('PER', 0, 1), ('LOC', 3, 3)]
        """
        # for nested list
        if any(isinstance(s, list) for s in seq):
            seq = [item for sublist in seq for item in sublist + ['O']]
        prev_tag = 'O'
        prev_type = ''
        begin_offset = 0
        chunks = []
        for i, chunk in enumerate(seq + ['O']):
            if suffix:
                tag = chunk[-1]
                type_ = chunk.split('-')[0]
            else:
                tag = chunk[0]
                type_ = chunk.split('-')[-1]

            if self.end_of_chunk(prev_tag, tag, prev_type, type_):
                chunks.append((prev_type, begin_offset, i-1))
            if self.start_of_chunk(prev_tag, tag, prev_type, type_):
                begin_offset = i
            prev_tag = tag
            prev_type = type_

        return chunks


    def end_of_chunk(self, prev_tag, tag, prev_type, type_):
        """Checks if a chunk ended between the previous and current word.

        Args:
            prev_tag: previous chunk tag.
            tag: current chunk tag.
            prev_type: previous type.
            type_: current type.

        Returns:
            chunk_end: boolean.
        """
        chunk_end = False

        if prev_tag == 'E': chunk_end = True
        if prev_tag == 'S': chunk_end = True

        if prev_tag == 'B' and tag == 'B': chunk_end = True
        if prev_tag == 'B' and tag == 'S': chunk_end = True
        if prev_tag == 'B' and tag == 'O': chunk_end = True
        if prev_tag == 'I' and tag == 'B': chunk_end = True
        if prev_tag == 'I' and tag == 'S': chunk_end = True
        if prev_tag == 'I' and tag == 'O': chunk_end = True

        if prev_tag != 'O' and prev_tag != '.' and prev_type != type_:
            chunk_end = True

        return chunk_end


    def start_of_chunk(self, prev_tag, tag, prev_type, type_):
        """Checks if a chunk started between the previous and current word.

        Args:
            prev_tag: previous chunk tag.
            tag: current chunk tag.
            prev_type: previous type.
            type_: current type.

        Returns:
            chunk_start: boolean.
        """
        chunk_start = False

        if tag == 'B': chunk_start = True
        if tag == 'S': chunk_start = True

        if prev_tag == 'E' and tag == 'E': chunk_start = True
        if prev_tag == 'E' and tag == 'I': chunk_start = True
        if prev_tag == 'S' and tag == 'E': chunk_start = True
        if prev_tag == 'S' and tag == 'I': chunk_start = True
        if prev_tag == 'O' and tag == 'E': chunk_start = True
        if prev_tag == 'O' and tag == 'I': chunk_start = True

        if tag != 'O' and tag != '.' and prev_type != type_:
            chunk_start = True

        return chunk_start


    def get_word_entities(self, word_list,words_entities):
        '''
        将分词过的实体开始结束位置转化为字的开始结束位置
        开始结束跟标注平台一致，包含开始位置，不包含结束位置
        '''
        word_entities = []
        for entity,start,end in words_entities:
            entity_json = {}
            start = len("".join(word_list[:start]))
            end = len("".join(word_list[:end+1]))
            entity_json["start"] = start
            entity_json["end"] = end
            entity_json["entityType"] = entity
            entity_json["entity"] = "".join(word_list)[start:end]
            word_entities.append(entity_json)
        return word_entities


    def get_ltp(self):
        if "ltp" not in globals():
            global ltp
            ltp = LtpParser() # 模型全局加载一次

        return ltp


    def release_ltp(self):
        if "ltp" in globals():
            del ltp


    def ltp_general_parser(self, sentences):
        parser_results = []
        ltp = self.get_ltp()
        for sentence in sentences:
            self.logger.info(f"开始处理句子 - {sentence}")
            parser_result = {}
            try:
                words = ltp.segmentor.segment(sentence)
                words_list = [ words[index] for index in range(len(words))]
                parser_result["tokens"] = words_list
                self.logger.info(f"分词完成")
            except Exception as e:
                self.logger.error(f"分词出错，{e}")
                parser_result["tokens"] = None
            try:
                postags = ltp.postagger.postag(words)
                postags_list = [postag for postag in postags]
                start = 0
                end = 0
                postags_json_list = []
                # 输出的start,end同标注平台一致，包括start，不包括end
                for i,word in enumerate(words_list):
                    postags_json = {}
                    end += len(word)
                    postags_json["start"] = start
                    postags_json["end"] = end
                    postags_json["word"] = words_list[i]
                    postags_json["postag"] = postags_list[i]
                    postags_json_list.append(postags_json)
                    start = end
                parser_result["postag"] = postags_json_list
                self.logger.info("词性标注完成")
            except Exception as e:
                self.logger.error(f"词性标注出错，{e}")
                parser_result["postag"] = None

            try:
                nertags = ltp.recognizer.recognize(words, postags)
                nertags_list = [tag for tag in nertags]
                words_entitys = self.get_entities(nertags_list)
                word_entitys = self.get_word_entities(words_list,words_entitys)
                parser_result["ner"] = word_entitys
                self.logger.info("NER标注完成")
            except Exception as e:
                self.logger.error(f"NER识别出错，{e}")
                parser_result["ner"] = None
            try:
                arcs = ltp.parser.parse(words, postags)
                arcs_json_list = []
                for i,arc in enumerate(arcs):
                    arcs_json = {}
                    headstart = -1 if (arc.head == 0) else len("".join(words_list[:arc.head-1]))
                    headend =  -1 if (arc.head == 0) else ( headstart + len(words_list[arc.head-1]) )
                    head = "root" if (arc.head == 0) else words_list[arc.head - 1]
                    tailstart = len("".join(words_list[:i]))
                    tailend = tailstart + len(words_list[i])
                    tail = words_list[i]
                    arcs_json = {
                                "headstart": headstart,
                                "headend": headend,
                                "head": head,
                                "tailstart": tailstart,
                                "tailend": tailend,
                                "tail": tail,
                                "relation": arc.relation
                                }
                    arcs_json_list.append(arcs_json)
                parser_result["arc"] = arcs_json_list
                self.logger.info("依存语法分析完成")
            except Exception as e:
                self.logger.error(f"依存语法分析出错,{e}")
                parser_result["arc"] = None
            try:
                items = ltp.labeller.label(words, postags, arcs)
                roles_json_list = []
                for item in items:
                    roles_json = {}
                    index = item.index
                    roles = item.arguments
                    roles_list = []
                    for role in roles:
                        start = len("".join(words_list[:role.range.start]))
                        end = len("".join(words_list[:role.range.end + 1]))
                        word = sentence[start:end]
                        srl = role.name
                        roles_list.append({"start": start, "end": end, "word": word, "role": srl})
                    roles_json_list.append({"index": index, "word": words_list[index], "roles": roles_list})
                parser_result["srl"] = roles_json_list
                self.logger.info("语义角色标注完成")
            except Exception as e:
                self.logger.error(f"语义角色标注出错，{e}")
                parser_result["srl"] = None
            parser_results.append(parser_result)        
        return parser_results
    
    def ltp_extract_triple(self, sentence):
        ltp = self.get_ltp()
        extractor = TripleExtractor(ltp)
        triples = extractor.triples_main(sentence)
        return triples




               





        

